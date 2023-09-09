
from pydantic import BaseModel

from src.command import Command
from dataclasses import fields
from pydantic.dataclasses import dataclass
from typing import Generic, TypeVar

CMD = TypeVar('CMD', bound=Command)


class CliCmdParser:

    def __init__(self, commands: list[Generic[CMD]]):
        self._commands = {cmd.__name__.lower(): cmd for cmd in commands}

    def parse(self, *args) -> CMD:
        cmdKey = args[0].replace('-', '').lower()
        cmd = self._commands[cmdKey]
        parsedArgs = dict()
        for name, field in cmd.__fields__.items():
            value = self._getParam(args, name)
            if value is None:
                if field.required:
                    raise Exception(
                        f"cannot find input for {name}, "
                        f"try flag --{name} or -{name[0]}")
                else:
                    continue
            parsedArgs[name] = value

        result = cmd(**parsedArgs)
        return result

    @staticmethod
    def _getParam(args: tuple, fieldName: str):
        patterns = (f"--{fieldName}", f"-{fieldName[0]}")

        index = None
        for pattern in patterns:
            matches = [idx for idx, arg in enumerate(args)
                       if arg.startswith(pattern)]
            if len(matches) == 1:
                index = matches[0]
                break
        if index is None:
            return
            # raise Exception(
            #     f"cannot find input for {fieldName}, "
            #     f"try flag {patterns[0]} or {patterns[1]}")

        result = args[index + 1]
        return result


if __name__ == "__main__":

    class SomeCmd(BaseModel):
        field1: str
        field2: int
        field3: bool
        gbb: str

    cliCmdParser = CliCmdParser([SomeCmd])
    cmd = cliCmdParser.parse(
        *("some-cmd", "--field2", "1", "--field1", "b", "--field3", "1", "-g", "dd")
    )
    assert isinstance(cmd, SomeCmd)
    assert cmd.field1 == "b"
    assert cmd.field2 == 1
    assert cmd.field3
