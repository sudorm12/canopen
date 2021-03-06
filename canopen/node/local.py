import logging
import struct

from .base import BaseNode
from ..sdo import SdoServer, SdoAbortedError
from ..pdo import PDO, TPDO, RPDO
from ..nmt import NmtSlave
from ..emcy import EmcyProducer
from .. import objectdictionary

logger = logging.getLogger(__name__)


class LocalNode(BaseNode):

    def __init__(self, node_id, object_dictionary):
        super(LocalNode, self).__init__(node_id, object_dictionary)

        self.data_store = {}
        self._read_callbacks = []
        self._write_callbacks = []

        self.sdo = SdoServer(0x600 + self.id, 0x580 + self.id, self)
        self.tpdo = TPDO(self)
        self.rpdo = RPDO(self)
        self.pdo = PDO(self, self.rpdo, self.tpdo)
        self.nmt = NmtSlave(self.id, self)
        # Let self.nmt handle writes for 0x1017
        self.add_write_callback(self.nmt.on_write)
        self.add_write_callback(self.tpdo.on_property_write)
        self.add_write_callback(self.tpdo.on_mapping_write)
        self.add_write_callback(self.tpdo.on_data_write)
        self.emcy = EmcyProducer(0x80 + self.id)
        for pdo_map in self.rpdo.values():
            pdo_map.add_callback(update_sdo_from_pdo)

        # read the pdo configuration and mapping
        self.pdo.read()

        # read tpdo data values from data dictionary
        for npdo, pdo_map in self.tpdo.items():
            for var in pdo_map:
                if var.od.subindex == 0:
                    sdo_data = self.sdo[var.od.index].data
                else:
                    sdo_data = self.sdo[var.od.index][var.od.subindex].data
                var.set_data(sdo_data)

    def associate_network(self, network):
        self.network = network
        self.sdo.network = network
        self.tpdo.network = network
        self.rpdo.network = network
        self.nmt.network = network
        self.emcy.network = network
        network.subscribe(self.sdo.rx_cobid, self.sdo.on_request)
        network.subscribe(0, self.nmt.on_command)

        for pdo_map in self.pdo.map.values():
            pdo_map.subscribe_to_network(network)

    def remove_network(self):
        self.network.unsubscribe(self.sdo.rx_cobid, self.sdo.on_request)
        self.network.unsubscribe(0, self.nmt.on_command)
        self.network = None
        self.sdo.network = None
        self.tpdo.network = None
        self.rpdo.network = None
        self.nmt.network = None
        self.emcy.network = None

    def add_read_callback(self, callback):
        self._read_callbacks.append(callback)

    def add_write_callback(self, callback):
        self._write_callbacks.append(callback)

    def get_data(self, index, subindex, check_readable=False):
        obj = self._find_object(index, subindex)

        if check_readable and not obj.readable:
            raise SdoAbortedError(0x06010001)

        # Try callback
        for callback in self._read_callbacks:
            result = callback(index=index, subindex=subindex, od=obj)
            if result is not None:
                return obj.encode_raw(result)

        # Try stored data
        try:
            return self.data_store[index][subindex]
        except KeyError:
            # Try ParameterValue in EDS
            if obj.value is not None:
                return obj.encode_raw(obj.value)
            # Try default value
            if obj.default is not None:
                return obj.encode_raw(obj.default)
            # if no value or default is available, just use zero
            return obj.encode_raw(0)

        # Resource not available
        logger.info("Resource unavailable for 0x%X:%d", index, subindex)
        raise SdoAbortedError(0x060A0023)

    def set_data(self, index, subindex, data, check_writable=False):
        obj = self._find_object(index, subindex)

        if check_writable and not obj.writable:
            raise SdoAbortedError(0x06010002)

        # Store data
        self.data_store.setdefault(index, {})
        self.data_store[index][subindex] = bytes(data)

        # Try callbacks
        for callback in self._write_callbacks:
            callback(index=index, subindex=subindex, od=obj, data=data)

    def _find_object(self, index, subindex):
        if index not in self.object_dictionary:
            # Index does not exist
            raise SdoAbortedError(0x06020000)
        obj = self.object_dictionary[index]
        if not isinstance(obj, objectdictionary.Variable):
            # Group or array
            if subindex not in obj:
                # Subindex does not exist
                raise SdoAbortedError(0x06090011)
            obj = obj[subindex]
        return obj


def update_sdo_from_pdo(map):
    for var in map:
        # var.raw = var.get_data()
        if var.subindex == 0:
            map.pdo_node.node.sdo[var.index].raw = var.raw
        else:
            map.pdo_node.node.sdo[var.index][var.subindex].raw = var.raw
