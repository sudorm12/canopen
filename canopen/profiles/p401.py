# inspired by the NmtMaster code
import logging
import time
from ..node import LocalNode
from ..sdo import SdoCommunicationError

logger = logging.getLogger(__name__)


class BaseNode401(LocalNode):
    """A CANopen CiA 401 profile slave node.

    :param int node_id:
        Node ID (set to None or 0 if specified by object dictionary)
    :param object_dictionary:
        Object dictionary as either a path to a file, an ``ObjectDictionary``
        or a file like object.
    :type object_dictionary: :class:`str`, :class:`canopen.ObjectDictionary`
    """

    def __init__(self, node_id, object_dictionary):
        super(BaseNode401, self).__init__(node_id, object_dictionary)
        self.tpdo_values = dict()  # { index: TPDO_value }
        self.rpdo_pointers = dict()  # { index: RPDO_pointer }
        self.add_write_callback(self.tpdo.on_property_write)

    def setup_402_state_machine(self):
        """Configure the state machine by searching for a TPDO that has the
        StatusWord mapped.
        :raise ValueError: If the the node can't find a Statusword configured
        in the any of the TPDOs
        """
        self.nmt.state = 'PRE-OPERATIONAL' # Why is this necessary?
        self.setup_pdos()
        self._check_controlword_configured()
        self._check_statusword_configured()
        self.nmt.state = 'OPERATIONAL'
        self.state = 'SWITCH ON DISABLED' # Why change state?

    def setup_pdos(self):
        self.pdo.read()  # TPDO and RPDO configurations
        # self._init_tpdo_values()
        # self._init_rpdo_pointers()

    def _init_tpdo_values(self):
        for tpdo in self.tpdo.values():
            if tpdo.enabled:
                tpdo.add_callback(self.on_TPDOs_update_callback)
                for obj in tpdo:
                    logger.debug('Configured TPDO: {0}'.format(obj.index))
                    if obj.index not in self.tpdo_values:
                        self.tpdo_values[obj.index] = 0

    def _init_rpdo_pointers(self):
        # If RPDOs have overlapping indecies, rpdo_pointers will point to 
        # the first RPDO that has that index configured.
        for rpdo in self.rpdo.values():
            for obj in rpdo:
                logger.debug('Configured RPDO: {0}'.format(obj.index))
                if obj.index not in self.rpdo_pointers:
                    self.rpdo_pointers[obj.index] = obj

    def on_TPDOs_update_callback(self, mapobject):
        """This function receives a map object.
        this map object is then used for changing the
        :param mapobject: :class: `canopen.objectdictionary.Variable`
        """
        for obj in mapobject:
            self.tpdo_values[obj.index] = obj.raw
