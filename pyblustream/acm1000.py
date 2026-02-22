"""ACM1000 device support."""

import asyncio
import aiohttp
from typing import Optional

from pyblustream.listener import MultiplexingListener
from pyblustream.protocol import ACM1000Protocol


class ACM1000:
    """Represents an ACM1000 control module."""

    def __init__(self, hostname, port):
        """Initialize ACM1000."""
        self.hostname: str = hostname
        self.port: int = port
        self._multiplex_callback = MultiplexingListener()
        self._protocol = ACM1000Protocol(hostname, port, self._multiplex_callback, heartbeat_time=60)
        self.outputs_by_id: dict[int, str] = {}
        self.outputs_by_name: dict[str, int] = {}
        self.inputs_by_id: dict[int, str] = {}
        self.inputs_by_name: dict[str, int] = {}
        self.mac: Optional[str] = None
        self.device_name: Optional[str] = None
        self.firmware_version: Optional[str] = None
        self.output_power_states: dict[int, bool] = {}  # Track per-output power state
        self.input_details: dict[int, dict] = {}  # Track input IP addresses and details
        self.output_details: dict[int, dict] = {}  # Track output IP addresses and details
        self._capture_number_cache: dict[str, int] = {}  # Cache: ip -> latest capture number
        self._cache_timestamps: dict[str, float] = {}  # Cache: ip -> timestamp

    @property
    def output_names(self):
        """Get list of output names."""
        return list(self.outputs_by_name.keys())

    @property
    def input_names(self):
        """Get list of input names."""
        return list(self.inputs_by_name.keys())

    async def async_connect(self):
        """Connect to ACM1000 and fetch metadata.

        Device names from ASTPARAM are fetched during connection to ensure
        entity names are correct from the start.
        """
        await self._protocol.async_connect()
        metadata_json = await self._get_matrix_metadata()
        self._process_meta_data(metadata_json)
        # Fetch real names via ASTPARAM commands
        await self._fetch_device_names()

    def close(self):
        """Close connection."""
        self._protocol.close()

    def change_source(self, input_id: int, output_id: int):
        """Change output source."""
        self._protocol.send_change_source(input_id, output_id)

    def change_source_by_name(self, input_name: str, output_name: str):
        """Change output source by name."""
        input_id = self.inputs_by_name(input_name)
        output_id = self.outputs_by_name(output_name)
        self._protocol.send_change_source(input_id, output_id)

    def update_status(self):
        """Request status update."""
        self._protocol.send_status_message()

    def status_of_output(self, output_id: int) -> Optional[int]:
        """Get current source of output."""
        return self._protocol.get_status_of_output(output_id)

    def status_of_all_outputs(self) -> list[tuple[int, Optional[int]]]:
        """Get status of all outputs."""
        return self._protocol.get_status_of_all_outputs()

    def turn_on_output(self, output_id: int):
        """Turn on specific output."""
        self._protocol.send_output_power(output_id, True)
        self.output_power_states[output_id] = True

    def turn_off_output(self, output_id: int):
        """Turn off specific output."""
        self._protocol.send_output_power(output_id, False)
        self.output_power_states[output_id] = False

    def is_output_on(self, output_id: int) -> bool:
        """Check if output is powered on."""
        return self.output_power_states.get(output_id, False)

    def turn_on(self):
        """ACM1000 does not support system power."""
        raise NotImplementedError("ACM1000 does not support system-level power control")

    def turn_off(self):
        """ACM1000 does not support system power."""
        raise NotImplementedError("ACM1000 does not support system-level power control")

    def is_on(self) -> bool:
        """ACM1000 does not have system power state."""
        raise NotImplementedError("ACM1000 does not have system-level power state")

    def register_listener(self, listener):
        """Register a listener for updates."""
        self._multiplex_callback.register_listener(listener)

    def unregister_listener(self, listener):
        """Unregister a listener."""
        self._multiplex_callback.unregister_listener(listener)

    def send_guest_command(self, guest_is_input, guest_id, command):
        """Send guest command to input or output."""
        return self._protocol.send_guest_command(guest_is_input, guest_id, command)

    def send_macro(self, macro_index: int):
        """Trigger macro with specified index."""
        return self._protocol.send_macro(macro_index)

    async def _fetch_device_name_via_astparam(self, device_type: str, device_id: int) -> str | None:
        """Fetch device name using ASTPARAM command.

        Args:
            device_type: "IN" for transmitter or "OUT" for receiver
            device_id: Device ID number

        Returns:
            Device name from astparam, or None if failed
        """
        import asyncio
        import re

        try:
            # Open telnet connection
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.hostname, self.port),
                timeout=5.0
            )

            # Wait for and consume the initial prompt (e.g., "ACM1000>")
            try:
                await asyncio.wait_for(reader.readuntil(b'>'), timeout=2.0)
            except (asyncio.TimeoutError, asyncio.LimitOverrunError):
                pass  # Continue even if we don't get a prompt

            # Send ASTPARAM command
            command = f"{device_type} {device_id:03d} ASTPARAM\r\n"
            writer.write(command.encode())
            await writer.drain()

            # Read response with timeout
            response_lines = []
            try:
                async with asyncio.timeout(2.0):
                    while True:
                        line = await reader.readline()
                        if not line:
                            break
                        decoded = line.decode('utf-8', errors='ignore').strip()
                        response_lines.append(decoded)
                        # Stop when we've read enough lines (astparam dump is usually < 100 lines)
                        if len(response_lines) > 100:
                            break
            except asyncio.TimeoutError:
                pass  # Normal - we read until timeout

            # Close connection
            writer.close()
            await writer.wait_closed()

            # Parse name from response
            # Look for line like: name=Tx Ads 5
            for line in response_lines:
                match = re.match(r'name=(.+)', line)
                if match:
                    return match.group(1).strip()

        except Exception as e:
            import logging
            logging.getLogger(__name__).debug(f"Failed to fetch name for {device_type} {device_id}: {e}")

        return None

    async def _fetch_device_names(self):
        """Fetch real device names using ASTPARAM commands.

        Adds a small delay between commands to avoid overwhelming the device.
        """
        import logging
        logger = logging.getLogger(__name__)

        # Fetch transmitter (input) names
        for input_id in list(self.inputs_by_id.keys()):
            old_name = self.inputs_by_id[input_id]
            name = await self._fetch_device_name_via_astparam("IN", input_id)
            if name:
                logger.info(f"Using ASTPARAM name for input {input_id}: '{name}'")
                # Update dictionaries
                self.inputs_by_id[input_id] = name
                if old_name in self.inputs_by_name:
                    del self.inputs_by_name[old_name]
                self.inputs_by_name[name] = input_id
            else:
                logger.warning(
                    f"Failed to fetch ASTPARAM name for input {input_id}, "
                    f"using metadata name: '{old_name}'"
                )
            # Small delay between commands to avoid overwhelming the device
            await asyncio.sleep(0.001)

        # Fetch receiver (output) names
        for output_id in list(self.outputs_by_id.keys()):
            old_name = self.outputs_by_id[output_id]
            name = await self._fetch_device_name_via_astparam("OUT", output_id)
            if name:
                logger.info(f"Using ASTPARAM name for output {output_id}: '{name}'")
                # Update dictionaries
                self.outputs_by_id[output_id] = name
                if old_name in self.outputs_by_name:
                    del self.outputs_by_name[old_name]
                self.outputs_by_name[name] = output_id
            else:
                logger.warning(
                    f"Failed to fetch ASTPARAM name for output {output_id}, "
                    f"using metadata name: '{old_name}'"
                )
            # Small delay between commands to avoid overwhelming the device
            await asyncio.sleep(0.001)

    async def _get_latest_capture_number(self, ip_address: str) -> int | None:
        """Find the highest numbered capture file for a given IP.

        ACM1000 stores images in /assets/SH/Image/{ip}/ as:
        - Transmitters (inputs): captureXXX.jpg (3 digits)
        - Receivers (outputs): captureX.jpg (single digit)
        We need to find the file with the highest number.

        Results are cached for 3 seconds to reduce HTTP requests.
        """
        import re
        import time

        # Check cache first (3 second TTL)
        current_time = time.time()
        if ip_address in self._cache_timestamps:
            cache_age = current_time - self._cache_timestamps[ip_address]
            if cache_age < 3.0:
                # Cache is still fresh, return cached value
                return self._capture_number_cache.get(ip_address)

        # Cache miss or stale, fetch from device
        url = f"http://{self.hostname}/assets/SH/Image/{ip_address}/"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=2)) as response:
                    if response.status == 200:
                        html = await response.text()

                        # Parse HTML directory listing to find capture files
                        # Pattern matches both formats: capture001.jpg, capture0.jpg, etc.
                        pattern = r'capture(\d+)\.jpg'
                        matches = re.findall(pattern, html)

                        if matches:
                            # Convert to integers and find max
                            numbers = [int(m) for m in matches]
                            latest = max(numbers)

                            # Update cache
                            self._capture_number_cache[ip_address] = latest
                            self._cache_timestamps[ip_address] = current_time
                            return latest
        except Exception:
            pass  # Return cached value if available, else None

        # Return cached value if available, else None
        return self._capture_number_cache.get(ip_address)

    async def async_get_input_image_url(self, input_id: int) -> str:
        """Get the latest input preview image URL.

        For ACM1000, finds the latest captureXXX.jpg file (transmitters use 3 digits).
        """
        ip_address = self.input_details.get(input_id, {}).get("ip", "")
        if not ip_address:
            return ""

        latest_num = await self._get_latest_capture_number(ip_address)
        if latest_num is not None:
            # Transmitters use captureXXX.jpg format (3 digits with leading zeros)
            return f"http://{self.hostname}/assets/SH/Image/{ip_address}/capture{latest_num:03d}.jpg"
        else:
            # Fallback to capture001.jpg if can't determine latest
            return f"http://{self.hostname}/assets/SH/Image/{ip_address}/capture001.jpg"

    async def async_get_output_image_url(self, output_id: int) -> str:
        """Get the latest output preview image URL.

        For ACM1000, finds the latest captureX.jpg file (receivers use single digit).
        """
        ip_address = self.output_details.get(output_id, {}).get("ip", "")
        if not ip_address:
            return ""

        latest_num = await self._get_latest_capture_number(ip_address)
        if latest_num is not None:
            # Receivers use captureX.jpg format (no leading zeros)
            return f"http://{self.hostname}/assets/SH/Image/{ip_address}/capture{latest_num}.jpg"
        else:
            # Fallback to capture0.jpg if can't determine latest
            return f"http://{self.hostname}/assets/SH/Image/{ip_address}/capture0.jpg"

    def get_input_image_url(self, input_id: int) -> str:
        """Return the base directory URL for input preview images.

        Note: For ACM1000, use async_get_input_image_url() instead to get the latest capture.
        This method returns the directory URL for compatibility.
        """
        ip_address = self.input_details.get(input_id, {}).get("ip", "")
        if not ip_address:
            return ""
        return f"http://{self.hostname}/assets/SH/Image/{ip_address}/"

    def get_output_image_url(self, output_id: int) -> str:
        """Return the base directory URL for output preview images.

        Note: For ACM1000, use async_get_output_image_url() instead to get the latest capture.
        This method returns the directory URL for compatibility.
        """
        ip_address = self.output_details.get(output_id, {}).get("ip", "")
        if not ip_address:
            return ""
        return f"http://{self.hostname}/assets/SH/Image/{ip_address}/"

    def get_initial_output_source_id(self, output_id: int) -> int:
        """Get initial source ID for output (for compatibility with base class)."""
        return self._protocol._output_to_input_map.get(output_id, 1)

    async def _get_matrix_metadata(self) -> dict:
        """Fetch ACM1000 config from HTTP endpoint."""
        url = f"http://{self.hostname}/assets/export/config.json"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.json()

    def _process_meta_data(self, metadata_json):
        """Process ACM1000-specific JSON metadata structure.

        ACM1000 structure:
        - syssta.devname: Device name (e.g., "ACM1000")
        - syssta.softver: Firmware version
        - Sys_Param.C_mac: Control LAN MAC address
        - Transmitters: Array of input devices
          - Id: Input ID
          - Name: Input name
          - IpAddress: Input IP
          - Status: 1=online, 0=offline
        - Receivers: Array of output devices
          - Id: Output ID
          - Name: Output name
          - IpAddress: Output IP
          - source_id: Current input source
          - video_output: 1=on, 0=off (power state)
          - status: 1=online, 0=offline
        """
        # System info
        syssta = metadata_json.get("syssta", {})
        sys_param = metadata_json.get("Sys_Param", {})

        self.device_name = syssta.get("devname", "")
        self.firmware_version = syssta.get("softver", "")
        self.mac = sys_param.get("C_mac", "")  # Control LAN MAC

        # Parse transmitters (inputs)
        transmitters = metadata_json.get("Transmitters", [])
        for tx in transmitters:
            tx_id = tx.get("Id")
            tx_name = tx.get("Name", "")
            tx_ip = tx.get("IpAddress", "")
            if tx_id is not None:
                self.inputs_by_id[tx_id] = tx_name
                self.inputs_by_name[tx_name] = tx_id
                # Store IP address for preview images
                self.input_details[tx_id] = {"ip": tx_ip}

        # Parse receivers (outputs)
        receivers = metadata_json.get("Receivers", [])
        for rx in receivers:
            rx_id = rx.get("Id")
            rx_name = rx.get("Name", "")
            rx_ip = rx.get("IpAddress", "")
            if rx_id is not None:
                self.outputs_by_id[rx_id] = rx_name
                self.outputs_by_name[rx_name] = rx_id

                # Store IP address for preview images
                self.output_details[rx_id] = {"ip": rx_ip}

                # Track initial power state
                self.output_power_states[rx_id] = rx.get("video_output", 0) == 1

                # Track current source
                source_id = rx.get("source_id")
                if source_id:
                    self._protocol._output_to_input_map[rx_id] = source_id
