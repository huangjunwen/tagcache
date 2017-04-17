# -*- encoding: utf-8 -*-

import io
import json
import pickle


class Serializer(object):

    def deserialize(self, io):
        """
        Deserialize from an io.BufferedIOBase object.

        :param io: io.BufferedIOBase instance.

        """
        raise NotImplementedError()

    def serialize(self, obj):
        """
        Serialize an object into io.BufferedIOBase object.

        :return: io.BufferedIOBase instance.

        """
        raise NotImplementedError()


class JSONSerializer(Serializer):
    """
    Serializer for JSON object.

    """
    def __init__(self, **dump_opt):

        dump_opt['separators'] = (',', ':')

        self.dump_opt = dump_opt

    def deserialize(self, io):

        return json.load(io)

    def serialize(self, obj):

        ret = io.BytesIO()

        json.dump(obj, ret, **self.dump_opt)

        return ret


class PickleSerializer(Serializer):
    """
    Serializer for python object.

    """
    def __init__(self, protocol=pickle.HIGHEST_PROTOCOL):

        self.protocol = protocol

    def deserialize(self, io):

        return pickle.load(io)

    def serialize(self, obj):

        ret = io.BytesIO()

        pickle.dump(obj, ret, protocol=self.protocol)

        return ret
