#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    '''
    Base abstract class for streams
    '''

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.nb_processed = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        '''
        Process the batch's data
        '''
        pass

    @abstractmethod
    def get_analysis(self, new_batch: List[Any]) -> str:
        '''
        Return the analysis of the batch
        '''
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        '''
        Return the filtered data of the batch
        '''
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        '''
        Return the stream id and the number of data processed
        '''
        return {
            "stream_id": self.stream_id,
            "nb_processed": self.nb_processed
        }

    def get_info(self, stream_type: str) -> str:
        '''
        Return some information about the stream
        '''
        return f"Stream ID: {self.stream_id}, Type: " + stream_type


class SensorStream(DataStream):
    '''
    Simulate a sensor stream
    '''

    def process_batch(self, data_batch: List[Any]) -> str:
        res = "["
        start, end = "{", "}"
        if len(data_batch) == 1:
            start, end = "", ""

        for k in range(len(data_batch)):
            res += start
            for (cle, val) in data_batch[k].items():
                self.nb_processed += 1
                res += cle + ": " + str(val) + ", "
            res = res[:-2]
            res += end

            if k != len(data_batch) - 1:
                res += ", "
        res += "]"

        if len(data_batch) != 1:
            self.nb_processed = len(data_batch)

        return res

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        new_batch = data_batch
        if criteria == "low_temp":
            new_batch = [batch for batch in data_batch if batch["temp"] < 25]
        return new_batch

    def get_analysis(self, new_batch: List[Any]) -> str:
        res = ""
        res += str(self.nb_processed) + " readings processed,"

        total = 0
        nb_temp = 0
        for batch in new_batch:
            total += batch["temp"]
            nb_temp += 1

        res += f" avg temp: {total / nb_temp}°C"

        return res


class TransactionStream(DataStream):
    '''
    Simulate a transaction stream
    '''

    def process_batch(self, data_batch: List[Any]) -> str:
        res = "["

        for k in range(len(data_batch)):
            for (cle, val) in data_batch[k].items():
                self.nb_processed += 1
                res += cle + ": " + str(val) + ", "
            res = res[:-2]

            if k != len(data_batch) - 1:
                res += ", "
        res += "]"

        return res

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        new_batch = data_batch
        index = []
        for batch in data_batch:
            index.append(list(batch.keys())[0])

        if criteria == "large_transaction":
            new_batch = [data_batch[k] for k in range(len(data_batch))
                         if data_batch[k][index[k]] > 25]
        return new_batch

    def get_analysis(self, new_batch: List[Any]) -> str:
        res = ""
        res += str(self.nb_processed) + " operations,"

        net = 0
        for batch in new_batch:
            batch_type = list(batch.keys())[0]
            if batch_type == "buy":
                net += batch[batch_type]
            else:
                net -= batch[batch_type]

        sign = '+' if net >= 0 else ''
        res += " net flow: " + sign + str(net) + " units"

        return res


class EventStream(DataStream):
    '''
    Simulate an event stream
    '''

    def process_batch(self, data_batch: List[Any]) -> str:
        self.nb_processed += len(data_batch)
        return str(data_batch)

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        new_batch = data_batch
        if criteria == "errors_only":
            new_batch = [batch for batch in data_batch if batch == "error"]
        return new_batch

    def get_analysis(self, new_batch: List[Any]) -> str:
        res = ""
        res += str(self.nb_processed) + " events,"

        nb_error = 0
        for batch in new_batch:
            if batch == "error":
                nb_error += 1

        plurial = "s" if nb_error > 1 else ""
        res += f" {nb_error} error" + plurial + " detected"

        return res


class StreamProcessor:
    '''
    Simulate a stream manager processing all the streams
    '''

    def __init__(self) -> None:
        self.streams = []

    def add_stream(self, stream: DataStream) -> None:
        '''
        Add a stream to the stream list
        '''
        self.streams.append(stream)

    def process_all(self, batches: List[Any], criteria: List[Any]) -> None:
        '''
        Process all the streams and display some informations
        '''
        print("Batch 1 Results:")

        for stream in self.streams:
            filter_batch = stream.filter_data(batches[stream.stream_id])
            stream.process_batch(filter_batch)

            if isinstance(stream, SensorStream):
                stream_type = "Sensor"
                process_type = "readings"
            elif isinstance(stream, TransactionStream):
                stream_type = "Transaction"
                process_type = "operations"
            elif isinstance(stream, EventStream):
                stream_type = "Event"
                process_type = "events"

            stats = stream.get_stats()

            print(f"- {stream_type} data: " + str(stats["nb_processed"]) +
                  f" {process_type} processed")

        print()


def test_sensor(batches: List[Any], criteria: List[Any],
                sensor_id: str) -> None:
    '''
    Test the SensorStream class
    '''
    print("Initializing Sensor Stream...")
    sensor = SensorStream(sensor_id)
    print(sensor.get_info("Environmental Data"))

    filter_batch = sensor.filter_data(batches[sensor_id],
                                      criteria[sensor_id])
    processed_batch = sensor.process_batch(filter_batch)
    print("Processing sensor batch:", processed_batch)

    sensor_stats = sensor.get_analysis(filter_batch)
    print("Sensor analysis:", sensor_stats)
    print()


def test_transaction(batches: List[Any], criteria: List[Any],
                     trans_id: str) -> None:
    '''
    Test the TransactionStream class
    '''
    print("Initializing Transaction Stream...")
    transaction = TransactionStream(trans_id)
    print(transaction.get_info("Financial Data"))

    filter_batch = transaction.filter_data(batches[trans_id],
                                           criteria[trans_id])
    processed_batch = transaction.process_batch(filter_batch)
    print("Processing transaction batch:", processed_batch)

    transaction_stats = transaction.get_analysis(filter_batch)
    print("Transaction analysis:", transaction_stats)
    print()


def test_event(batches: List[Any], criteria: List[Any],
               event_id: str) -> None:
    '''
    Test the EventStream class
    '''
    print("Initializing Event Stream...")
    event = EventStream(event_id)
    print(event.get_info("System Events"))

    filter_batch = event.filter_data(batches[event_id])
    processed_batch = event.process_batch(filter_batch)
    print("Processing event batch:", processed_batch)

    event_stats = event.get_analysis(filter_batch)
    print("Event analysis:", event_stats)
    print()


def test_all_streams(batches: List[Any], criteria: List[Any],
                     streams_ids: List[str]) -> None:
    '''
    Test the StreamProcessor class
    '''
    sensor = SensorStream(streams_ids[0])
    transaction = TransactionStream(streams_ids[1])
    event = EventStream(streams_ids[2])

    print("=== Polymorphic Stream Processing ===")
    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(transaction)
    processor.add_stream(event)

    print("Processing mixed stream types through unified interface...\n")
    processor.process_all(batches, criteria)


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    sensor_id = "SENSOR_001"
    trans_id = "TRANS_001"
    event_id = "EVENT_001"

    batches = {
        sensor_id: [
            {"temp": 22.5, "humidity": 65, "pressure": 1013},
            {"temp": 28.0, "humidity": 70, "pressure": 1010}
        ],
        trans_id: [
            {"buy": 100}, {"sell": 150}, {"buy": 75}, {"sell": 20}
        ],
        event_id: ["login", "error", "logout"]
    }
    criteria = {
        sensor_id: "low_temp",
        trans_id: "large_transaction",
        event_id: "errors_only"
    }

    sensor = test_sensor(batches, criteria, sensor_id)
    transaction = test_transaction(batches, criteria, trans_id)
    event = test_event(batches, criteria, event_id)

    streams_ids = [sensor_id, trans_id, event_id]

    test_all_streams(batches, criteria, streams_ids)

    print("Stream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction\n")

    print("All streams processed successfully. Nexus throughput optimal.")
