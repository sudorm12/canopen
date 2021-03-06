from .base import PdoBase, Maps, Map, Variable

import logging
import itertools
import struct
import canopen

logger = logging.getLogger(__name__)


class PDO(PdoBase):
    """PDO Class for backwards compatibility
    :param rpdo: RPDO object holding the Receive PDO mappings
    :param tpdo: TPDO object holding the Transmit PDO mappings
    """

    def __init__(self, node, rpdo, tpdo):
        super(PDO, self).__init__(node)
        self.rx = rpdo.map
        self.tx = tpdo.map

        self.map = {}
        # the object 0x1A00 equals to key '1' so we remove 1 from the key
        for key, value in self.rx.items():
            self.map[0x1A00 + (key - 1)] = value
        for key, value in self.tx.items():
            self.map[0x1600 + (key - 1)] = value


class RPDO(PdoBase):
    """PDO specialization for the Receive PDO enabling the transfer of data from the master to the node.
    Properties 0x1400 to 0x1403 | Mapping 0x1600 to 0x1603.
    :param object node: Parent node for this object."""

    def __init__(self, node):
        super(RPDO, self).__init__(node)
        self.map = Maps(0x1400, 0x1600, self, 0x200)
        logger.debug('RPDO Map as {0}'.format(len(self.map)))

    def stop(self):
        """Stop transmission of all RPDOs.
        :raise TypeError: Exception is thrown if the node associated with the PDO does not
        support this function"""
        if isinstance(self.node, canopen.RemoteNode):
            for pdo in self.map.values():
                pdo.stop()
        else:
            raise TypeError('The node type does not support this function.')


class TPDO(PdoBase):
    """PDO specialization for the Transmit PDO enabling the transfer of data from the node to the master.
    Properties 0x1800 to 0x1803 | Mapping 0x1A00 to 0x1A03."""

    def __init__(self, node):
        super(TPDO, self).__init__(node)
        self.map = Maps(0x1800, 0x1A00, self, 0x180)
        logger.debug('TPDO Map as {0}'.format(len(self.map)))

    def stop(self):
        """Stop transmission of all TPDOs.
        :raise TypeError: Exception is thrown if the node associated with the PDO does not
        support this function"""
        if isinstance(self.node, canopen.LocalNode):
            logging.debug('Stopping all TPDOs')
            for pdo in self.map.values():
                pdo.stop()
        else:
            raise TypeError('The node type does not support this function.')

    def start_all(self):
        if isinstance(self.node, canopen.LocalNode):
            logging.debug('Starting all TPDOs')
            for npdo, pdo_map in self.map.items():
                if pdo_map.trans_type >= 254:
                    pdo_map.start()
        else:
            raise TypeError('The node type does not support this function.')

    def on_property_write(self, index, data, subindex=None, **kwargs):
        for npdo, pdo_map in self.map.items():
            # check if SDO index matches PDO configuration object id
            pdo_param_index = pdo_map.com_record.od.index
            if index == pdo_param_index:
                # read updated configuration from object dictionary
                pdo_map.read_config()
                logging.debug('Updating configuration for TPDO{}'.format(npdo))

                # start PDOs if node is operational and transmission type is 254 or 255
                if self.node.nmt.state == 'OPERATIONAL' and pdo_map.trans_type >= 254:
                    pdo_map.start()

    def on_mapping_write(self, index, data, subindex=None, **kwargs):
        for npdo, pdo_map in self.map.items():
            # check if SDO index matches PDO mapping object id
            pdo_map_index = pdo_map.map_array.od.index
            if index == pdo_map_index:
                logging.debug('Updating mapping for TPDO{}'.format(npdo))

                # read updated mapping from object dictionary
                pdo_map.read_mapping()

                # use mapped SDO variable to set PDO variable data
                for var in pdo_map:
                    if var.od.subindex == 0:
                        sdo_data = self.node.sdo[var.od.index].data
                    else:
                        sdo_data = self.node.sdo[var.od.index][var.od.subindex].data
                    var.set_data(sdo_data)

                # update PDO data
                pdo_map.update()

    def on_data_write(self, index, data, subindex=None, **kwargs):
        for npdo, pdo_map in self.map.items():
            # check if a mapped variable was updated
            for var in pdo_map:
                if index == var.index:
                    # update PDO data
                    var.set_data(data)
                    logging.debug('Updating data for TPDO{}'.format(npdo))
