#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataProcessor(ABC):

    def __init__(self, data: Optional[Any]):
        self.data = data

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def format_output(self, result: str) -> str:
        pass


class NumericProcessor(DataProcessor):

    def __init__(self, data: Optional[List[Union[int, str]]] = None):
        super().__init__(data)

    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "Validation ERROR"

        s = 0
        d_len = len(data)
        for num in data:
            s += num
        return f"Processed {d_len} numeric values, sum={s}, avg={s / d_len}"

    def validate(self, data: Any) -> bool:
        for elem in data:
            try:
                if int(elem) != elem:
                    return False
            except ValueError:
                return False
        return True

    def format_output(self, result: str) -> str:
        return "Output: " + result


class TextProcessor(DataProcessor):

    def __init__(self, data: Optional[List[Union[int, str]]] = None):
        super().__init__(data)

    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "Validation ERROR"

        c, w = 0, 0
        for k in range(len(data)):
            if data[k].isalpha():
                c += 1
                if k == len(data) - 1 or data[k + 1] == " ":
                    w += 1

        return f"Processed text: {c} characters, {w} words"

    def validate(self, data: Any) -> bool:
        for elem in data:
            try:
                if str(elem) != elem:
                    return False
            except ValueError:
                return False
        return True

    def format_output(self, result: str) -> str:
        return "Output: " + result


def get_type_index(data: str) -> int:
    i = 1
    while data[i].isupper():
        i += 1
    return i


class LogProcessor(DataProcessor):

    def __init__(self, data: Optional[List[Union[int, str]]] = None):
        super().__init__(data)

    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "Validation ERROR"

        ind = get_type_index(data)

        r_type = data[0:ind]
        if r_type == "ERROR":
            r_type = "ALERT"

        return f"[{r_type}] {data[0:ind]} level detected: " + data[ind + 2:]

    def validate(self, data: Any) -> bool:
        for elem in data:
            try:
                if str(elem) != elem:
                    return False
            except ValueError:
                return False
        return True

    def format_output(self, result: str) -> str:
        return "Output: " + result


def test_numeric(datas: Dict) -> None:
    data = datas["numeric"]
    print("Initializing Numeric Processor...")
    obj = NumericProcessor(data)
    print(f"Processing data: {data}")

    if obj.validate(data):
        print("Validation: Numeric data verified")

        result = obj.process(data)
        print(obj.format_output(result))
    else:
        print("Validation: Error")

    print()


def test_text(datas: Dict) -> None:
    data = datas["text"]
    print("Initializing Text Processor...")
    obj = TextProcessor(data)
    print(f"Processing data: \"{data}\"")

    if obj.validate(data):
        print("Validation: Text data verified")

        result = obj.process(data)
        print(obj.format_output(result))
    else:
        print("Validation: Error")

    print()


def test_log(datas: Dict) -> None:
    data = datas["log"]
    print("Initializing Log Processor...")
    obj = LogProcessor(data)
    print(f"Processing data: \"{data}\"")

    if obj.validate(data):
        print("Validation: Log entry verified")

        result = obj.process(data)
        print(obj.format_output(result))
    else:
        print("Validation: Error")

    print()


def test_all(datas: Dict) -> None:
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    data1 = datas["numeric"]
    data2 = datas["text"]
    data3 = datas["log"]

    obj1 = NumericProcessor()
    obj2 = TextProcessor()
    obj3 = LogProcessor()

    res1 = obj1.process(data1)
    print("Result 1:", obj1.format_output(res1))

    res2 = obj2.process(data2)
    print("Result 2:", obj2.format_output(res2))

    res3 = obj3.process(data3)
    print("Result 3:", obj3.format_output(res3))
    print()


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    datas = {
        "numeric": [1, 2, 3, 4, 5],
        "text": "Hello Nexus World",
        "log": "ERROR: Connection timeout"
    }
    test_numeric(datas)
    test_text(datas)
    test_log(datas)
    test_all(datas)

    print("Foundation systems online. Nexus ready for advanced streams.")
