from a2.server import ServerFactory, Server
from twisted.trial import unittest
from twisted.test import proto_helpers



class ServerTestCase(unittest.TestCase):
    # ERROR txn_id seq_num error_code length
    #
    # reason
    _error_string = "ERROR %d 0 %d %d\r\n\r\n%s\r\n"

    def setUp(self):
        factory = ServerFactory('~')
        self.proto = factory.buildProtocol(('127.0.0.1', 1234))
        self.tr = proto_helpers.StringTransport()
        self.proto.makeConnection(self.tr)

    def _test(self, send, expected):
        self.proto.dataReceived(send)
        self.assertEqual(self.tr.value(), expected)

    def tearDown(self):
        self.proto.connectionLost("Done test")


    def test_header_wrong_length(self):
        send = "WRITE 1\r\n"
        err = "Header is wrong length"
        expected = self._error_string % (-1, 204, len(err), err)
        return self._test(send, expected)

    def test_txn_not_number(self):
        send = "WRITE a 1 0\r\n"
        err = "Header has non-numeric value"
        expected = self._error_string % (-1, 204, len(err), err)
        return self._test(send, expected)

    def test_seq_not_number(self):
        send = "WRITE 1 [5] 0\r\n"
        err = "Header has non-numeric value"
        expected = self._error_string % (1, 204, len(err), err)
        return self._test(send, expected)

    # def test_len_too_short(self):
    #     send = "WRITE a 1 0\r\n"
    #     err = "Header is wrong length"
    #     expected = self._error_string % (-1, 204, len(err), err)
    #     return self._test(send, expected)


    def test_test(self):
        send = "WRITE -1 0 44\r\n\r\nWRITE -1 0 0 Mon, 13 Aug 2012 15:17:45 -0700"
        expected = "Method: WRITE, txn: -1, seq: 0, buf: WRITE -1 0 0 Mon, 13 Aug 2012 15:17:45 -0700\r\n"
        return self._test(send, expected)



