import logging
import struct

from canopen.sdo.base import SdoBase
from canopen.sdo.constants import *
from canopen.sdo.exceptions import *


logger = logging.getLogger(__name__)


class SdoServer(SdoBase):
    """Creates an SDO server."""

    def __init__(self, rx_cobid, tx_cobid, node):
        """
        :param int rx_cobid:
            COB-ID that the server receives on (usually 0x600 + node ID)
        :param int tx_cobid:
            COB-ID that the server responds with (usually 0x580 + node ID)
        :param canopen.LocalNode od:
            Node object owning the server
        """
        SdoBase.__init__(self, rx_cobid, tx_cobid, node.object_dictionary)
        self._node = node
        self._buffer = None
        self._toggle = 0
        self._index = None
        self._subindex = None
        self.last_received_error = 0x00000000
        
        # Block transfer state
        self._block_sequence = 0
        self._block_size = 127  # Default block size
        self._block_crc = None
        self._block_crc_supported = False
        self._block_data = None
        self._block_total_size = 0
        self._block_sent_segments = []  # Keep track of sent segments for retransmission
        self._block_mode = False  # Flag to track if we're in block transfer mode
        self._last_segment_size = 0  # Track the actual data size of the last segment

    def on_request(self, can_id, data, timestamp):
        command, = struct.unpack_from("B", data, 0)
        ccs = command & 0xE0
        logger.debug("=== on_request: command=0x%02X, ccs=0x%02X ===", command, ccs)

        try:
            if ccs == REQUEST_UPLOAD:
                self.init_upload(data)
            elif ccs == REQUEST_SEGMENT_UPLOAD:
                self.segmented_upload(command)
            elif ccs == REQUEST_DOWNLOAD:
                self.init_download(data)
            elif ccs == REQUEST_SEGMENT_DOWNLOAD:
                # Check if we're in block download mode
                if self._block_mode:
                    self.block_download(data)
                else:
                    self.segmented_download(command, data)
            elif ccs == REQUEST_BLOCK_UPLOAD:
                self.block_upload(data)
            elif ccs == REQUEST_BLOCK_DOWNLOAD:
                self.block_download(data)
            elif ccs == REQUEST_ABORTED:
                # Check if this might be a block segment with last bit set
                if self._block_mode and (command & 0x7F) > 0:
                    logger.debug("Routing last block segment (command=0x%02X) to block_download", command)
                    self.block_download(data)
                else:
                    self.request_aborted(data)
            else:
                # Check if this could be a block download segment
                if self._block_mode and (command & 0xE0) == 0:
                    self.block_download(data)
                else:
                    self.abort(ABORT_INVALID_COMMAND_SPECIFIER)
        except SdoAbortedError as exc:
            self.abort(exc.code)
        except KeyError as exc:
            self.abort(ABORT_NOT_IN_OD)
        except Exception as exc:
            self.abort()
            logger.exception(exc)

    def init_upload(self, request):
        _, index, subindex = SDO_STRUCT.unpack_from(request)
        self._index = index
        self._subindex = subindex
        res_command = RESPONSE_UPLOAD | SIZE_SPECIFIED
        response = bytearray(8)

        # Reset block mode when starting regular upload
        self._block_mode = False

        data = self._node.get_data(index, subindex, check_readable=True)
        size = len(data)
        if size == 0:
            logger.info("No content to upload for 0x%04X:%02X", index, subindex)
            self.abort(ABORT_NO_DATA_AVAILABLE)
            return
        elif size <= 4:
            logger.info("Expedited upload for 0x%04X:%02X", index, subindex)
            res_command |= EXPEDITED
            res_command |= (4 - size) << 2
            response[4:4 + size] = data
        else:
            logger.info("Initiating segmented upload for 0x%04X:%02X", index, subindex)
            struct.pack_into("<L", response, 4, size)
            self._buffer = bytearray(data)
            self._toggle = 0

        SDO_STRUCT.pack_into(response, 0, res_command, index, subindex)
        self.send_response(response)

    def segmented_upload(self, command):
        if self._buffer is None:
            logger.error("No buffer initialized for segmented upload")
            self.abort(ABORT_GENERAL_ERROR)
            return
            
        if command & TOGGLE_BIT != self._toggle:
            # Toggle bit mismatch
            raise SdoAbortedError(ABORT_TOGGLE_NOT_ALTERNATED)
        data = self._buffer[:7]
        size = len(data)

        # Remove sent data from buffer
        del self._buffer[:7]

        res_command = RESPONSE_SEGMENT_UPLOAD
        # Add toggle bit
        res_command |= self._toggle
        # Add nof bytes not used
        res_command |= (7 - size) << 1
        if not self._buffer:
            # Nothing left in buffer
            res_command |= NO_MORE_DATA
        # Toggle bit for next message
        self._toggle ^= TOGGLE_BIT

        response = bytearray(8)
        response[0] = res_command
        response[1:1 + size] = data
        self.send_response(response)

    def block_upload(self, data):
        """Handle block upload initiate request."""
        command = data[0]
        
        # Check if this is an initiate, start, or end command (bits 7-5 = 101)  
        if (command & 0xE0) == REQUEST_BLOCK_UPLOAD:
            # Check bits 1-0 for sub-command
            sub_cmd = command & 0x03
            
            if sub_cmd == INITIATE_BLOCK_TRANSFER:
                # Only for initiate we parse index/subindex
                _, index, subindex = SDO_STRUCT.unpack_from(data)
                self._index = index
                self._subindex = subindex
                self._init_block_upload(command, data)
            elif sub_cmd == START_BLOCK_UPLOAD:
                self._start_block_upload(data)
            elif sub_cmd == BLOCK_TRANSFER_RESPONSE:
                self._handle_block_upload_ack(data)
            elif sub_cmd == END_BLOCK_TRANSFER:
                self._end_block_upload()
            else:
                self.abort(ABORT_INVALID_COMMAND_SPECIFIER)
        else:
            self.abort(ABORT_INVALID_COMMAND_SPECIFIER)

    def request_aborted(self, data):
        _, index, subindex, code = struct.unpack_from("<BHBL", data)
        self.last_received_error = code
        logger.info("Received request aborted for 0x%04X:%02X with code 0x%X", index, subindex, code)

    def block_download(self, data):
        """Handle block download request."""
        command, index, subindex = SDO_STRUCT.unpack_from(data)
        
        # Check if this is an initiate or end command (bits 7-5 = 110)
        if (command & 0xE0) == REQUEST_BLOCK_DOWNLOAD:
            # Check bit 0 to distinguish initiate (0) from end (1)
            if (command & 0x01) == 0:
                # Initiate block download
                self._index = index
                self._subindex = subindex
                self._init_block_download(command, data)
            else:
                # End block download - preserve original index/subindex
                self._end_block_download(command, data)
        else:
            # Block download segment
            self._handle_block_download_segment(command, data)

    def init_download(self, request):
        # TODO: Check if writable (now would fail on end of segmented downloads)
        command, index, subindex = SDO_STRUCT.unpack_from(request)
        self._index = index
        self._subindex = subindex
        res_command = RESPONSE_DOWNLOAD
        response = bytearray(8)

        # Reset block mode when starting regular download
        self._block_mode = False

        if command & EXPEDITED:
            logger.info("Expedited download for 0x%04X:%02X", index, subindex)
            if command & SIZE_SPECIFIED:
                size = 4 - ((command >> 2) & 0x3)
            else:
                size = 4
            self._node.set_data(index, subindex, request[4:4 + size], check_writable=True)
        else:
            logger.info("Initiating segmented download for 0x%04X:%02X", index, subindex)
            if command & SIZE_SPECIFIED:
                size, = struct.unpack_from("<L", request, 4)
                logger.info("Size is %d bytes", size)
            self._buffer = bytearray()
            self._toggle = 0

        SDO_STRUCT.pack_into(response, 0, res_command, index, subindex)
        self.send_response(response)

    def segmented_download(self, command, request):
        if command & TOGGLE_BIT != self._toggle:
            # Toggle bit mismatch
            raise SdoAbortedError(ABORT_TOGGLE_NOT_ALTERNATED)
        last_byte = 8 - ((command >> 1) & 0x7)
        self._buffer.extend(request[1:last_byte])

        if command & NO_MORE_DATA:
            self._node.set_data(self._index,
                                self._subindex,
                                self._buffer,
                                check_writable=True)

        res_command = RESPONSE_SEGMENT_DOWNLOAD
        # Add toggle bit
        res_command |= self._toggle
        # Toggle bit for next message
        self._toggle ^= TOGGLE_BIT

        response = bytearray(8)
        response[0] = res_command
        self.send_response(response)

    def send_response(self, response):
        logger.debug("Sending response: %s", ' '.join(f'0x{b:02X}' for b in response))
        self.network.send_message(self.tx_cobid, response)

    def abort(self, abort_code=ABORT_GENERAL_ERROR):
        """Abort current transfer."""
        data = struct.pack("<BHBL", RESPONSE_ABORTED,
                           self._index, self._subindex, abort_code)
        self.send_response(data)
        # logger.error("Transfer aborted with code 0x%08X", abort_code)

    def upload(self, index: int, subindex: int) -> bytes:
        """May be called to make a read operation without an Object Dictionary.

        :param index:
            Index of object to read.
        :param subindex:
            Sub-index of object to read.

        :return: A data object.

        :raises canopen.SdoAbortedError:
            When node responds with an error.
        """
        return self._node.get_data(index, subindex)

    def download(
        self,
        index: int,
        subindex: int,
        data: bytes,
        force_segment: bool = False,
    ):
        """May be called to make a write operation without an Object Dictionary.

        :param index:
            Index of object to write.
        :param subindex:
            Sub-index of object to write.
        :param data:
            Data to be written.

        :raises canopen.SdoAbortedError:
            When node responds with an error.
        """
        return self._node.set_data(index, subindex, data)

    def _init_block_upload(self, command, request):
        """Initialize block upload."""
        # Check if CRC is supported
        client_crc_support = bool(command & CRC_SUPPORTED)
        
        # Get the requested block size from client
        client_blksize = request[4] if len(request) > 4 else 127
        
        # Use the requested block size (limit to reasonable values)
        self._block_size = min(max(client_blksize, 1), 127)
        
        # Get data from object dictionary
        try:
            data = self._node.get_data(self._index, self._subindex, check_readable=True)
        except Exception:
            self.abort(ABORT_NOT_IN_OD)
            return
            
        size = len(data)
        if size == 0:
            self.abort(ABORT_NO_DATA_AVAILABLE)
            return
        
        # Store data for block transfer
        self._block_data = bytearray(data)
        self._block_total_size = size
        self._block_sequence = 0
        self._block_sent_segments = []
        self._block_mode = True
        
        # Initialize CRC if supported by both client and server
        self._block_crc_supported = client_crc_support
        if self._block_crc_supported:
            self._block_crc = self.crc_cls()
        
        # Send initiate response
        response = bytearray(8)
        res_command = RESPONSE_BLOCK_UPLOAD | INITIATE_BLOCK_TRANSFER
        if self._block_crc_supported:
            res_command |= CRC_SUPPORTED
        res_command |= BLOCK_SIZE_SPECIFIED
        
        SDO_STRUCT.pack_into(response, 0, res_command, self._index, self._subindex)
        struct.pack_into("<L", response, 4, size)
        
        self.send_response(response)

    def _start_block_upload(self, request):
        """Start sending block upload segments."""
        self._send_block_upload_segments()

    def _send_block_upload_segments(self):
        """Send a block of upload segments."""
        segments_sent = 0
        
        while segments_sent < self._block_size and self._block_data:
            self._block_sequence += 1
            segments_sent += 1
            
            # Get up to 7 bytes of data
            segment_data = self._block_data[:7]
            del self._block_data[:7]
            
            # Track the actual data size for the last segment
            if not self._block_data:
                self._last_segment_size = len(segment_data)
            
            # Build segment
            response = bytearray(8)
            res_command = self._block_sequence
            
            # Check if this is the last segment of the entire transfer
            if not self._block_data:
                res_command |= NO_MORE_BLOCKS
                
            response[0] = res_command
            response[1:1 + len(segment_data)] = segment_data
            
            # Store segment for potential retransmission
            self._block_sent_segments.append((self._block_sequence, bytes(response)))
            
            # Update CRC if enabled
            if self._block_crc_supported and self._block_crc is not None:
                self._block_crc.process(segment_data)
            
            self.send_response(response)

    def _handle_block_upload_ack(self, request):
        """Handle block upload acknowledgment from client."""
        ackseq = request[1] if len(request) > 1 else 0
        blksize = request[2] if len(request) > 2 else self._block_size
        
        # Update block size for next block
        self._block_size = min(max(blksize, 1), 127)
        
        # Check if client received all segments we sent
        if ackseq < self._block_sequence:
            # Retransmit missing segments
            segments_to_send = []
            for seq_num, segment_data in self._block_sent_segments:
                if seq_num > ackseq:
                    segments_to_send.append((seq_num, segment_data))
            
            # Sort by sequence number to ensure correct order
            segments_to_send.sort(key=lambda x: x[0])
            
            # Send all missing segments
            for seq_num, segment_data in segments_to_send:
                self.send_response(segment_data)
            
            return
            
        # All segments were received, clear the sent segments buffer
        self._block_sent_segments = []
        
        # Reset sequence counter for next block
        self._block_sequence = 0
        
        # Send next block if there's more data
        if self._block_data:
            self._send_block_upload_segments()
        else:
            # No more data, send end block upload response
            self._end_block_upload()

    def _end_block_upload(self):
        """Handle end of block upload."""
        # Send end block upload response with CRC if supported
        response = bytearray(8)
        res_command = RESPONSE_BLOCK_UPLOAD | END_BLOCK_TRANSFER
        
        # Calculate number of unused bytes in last segment
        unused_bytes = 7 - self._last_segment_size if self._last_segment_size > 0 else 0
        
        # Encode unused bytes in bits 4-2 of the command
        res_command |= (unused_bytes & 0x7) << 2
        
        if self._block_crc_supported and self._block_crc:
            crc_value = self._block_crc.final()
            struct.pack_into("<H", response, 1, crc_value)
        
        response[0] = res_command
        self.send_response(response)
        
        # Clean up
        self._block_data = None
        self._block_crc = None
        self._block_mode = False

    def _init_block_download(self, command, request):
        """Initialize block download."""
        # Check if CRC is supported
        client_crc_support = bool(command & CRC_SUPPORTED)
        
        # Check if size is specified
        if command & BLOCK_SIZE_SPECIFIED:
            size, = struct.unpack_from("<L", request, 4)
            self._block_total_size = size
        else:
            self._block_total_size = 0
            
        # Initialize for block download
        self._block_data = bytearray()
        self._block_sequence = 0
        self._block_size = 127
        self._block_mode = True
        
        # Initialize CRC if supported
        self._block_crc_supported = client_crc_support
        if self._block_crc_supported:
            self._block_crc = self.crc_cls()
        
        # Send initiate response
        response = bytearray(8)
        res_command = RESPONSE_BLOCK_DOWNLOAD | INITIATE_BLOCK_TRANSFER
        if self._block_crc_supported:
            res_command |= CRC_SUPPORTED
            
        SDO_STRUCT.pack_into(response, 0, res_command, self._index, self._subindex)
        response[4] = self._block_size
        
        self.send_response(response)

    def _handle_block_download_segment(self, command, request):
        """Handle a block download segment."""
        sequence = command & 0x7F
        is_last = bool(command & 0x80)
        
        # Check sequence number
        if sequence != self._block_sequence + 1:
            self.abort(ABORT_INVALID_SEQUENCE_NUMBER)
            return
            
        self._block_sequence = sequence
        
        # Extract data (bytes 1-7)
        segment_data = request[1:8]
            
        # Add to buffer
        if self._block_data is not None:
            self._block_data.extend(segment_data)
        
        # Update CRC if enabled
        if self._block_crc_supported and self._block_crc is not None:
            self._block_crc.process(segment_data)
        
        # Check if we should send an ACK
        if sequence >= self._block_size or is_last:
            # Send block acknowledgment
            response = bytearray(8)
            res_command = RESPONSE_BLOCK_DOWNLOAD | BLOCK_TRANSFER_RESPONSE
            
            response[0] = res_command
            response[1] = sequence
            response[2] = self._block_size
            
            self.send_response(response)
            
            # Reset sequence for next block
            self._block_sequence = 0

    def _end_block_download(self, command, request):
        """Handle end of block download."""
        # Extract number of unused bytes in last segment
        n = (command >> 2) & 0x07
        
        # Remove unused bytes from the end
        if n > 0:
            for _ in range(n):
                if self._block_data:
                    self._block_data.pop()
        
        # Recalculate CRC over the actual data (without padding)
        if self._block_crc_supported and self._block_crc and self._block_data:
            self._block_crc = self.crc_cls()
            self._block_crc.process(self._block_data)
        
        # Extract and verify CRC if present
        if self._block_crc_supported and len(request) >= 3:
            received_crc, = struct.unpack_from("<H", request, 1)
            
            if self._block_crc:
                calculated_crc = self._block_crc.final()
                if received_crc != calculated_crc:
                    self.abort(ABORT_CRC_ERROR)
                    return
        
        # Write data to object dictionary
        if self._block_data is not None:
            try:
                self._node.set_data(self._index, self._subindex, bytes(self._block_data), check_writable=True)
            except Exception as e:
                self.abort(ABORT_STORE_APPLICATION)
                return
        else:
            self.abort(ABORT_GENERAL_ERROR)
            return
        
        # Send final response
        response = bytearray(8)
        res_command = RESPONSE_BLOCK_DOWNLOAD | END_BLOCK_TRANSFER
        response[0] = res_command
        
        self.send_response(response)
        
        # Clean up
        self._block_data = None
        self._block_crc = None
        self._block_mode = False
