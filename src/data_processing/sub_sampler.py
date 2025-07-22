"""Module that stores data seen by context id and category
in order to determine if message will be forwarded to
output unit"""
from datetime import datetime, timedelta
from typing import Dict, Any
import product_common_logging as logging

from data_processing.models.unified_model import UnifiedModel


class SubSampler():
    """Class that offers a dictionnary and methods
    to determine if a message will be forwarded or not"""

    def __init__(self, sub_sampling_period: int, timeout_period):
        """Init method used to instantiate the
        required settings of the sub sampler"""

        self.sub_sampling_period = sub_sampling_period
        self.timeout_period = timeout_period
        self.messages: Dict["str", Any] = {}
        self.last_time_cleanup_was_performed = datetime.utcnow()

    def should_process(self, message: UnifiedModel):
        """Method that determine if a message will be processed or not
        This is allowed if a new ctx_id to obj_id combination or if
        existing key sub_sampling period has elapsed"""
        obj_id = str(message.snapshot.obj)

        if (f"{obj_id}" not in self.messages or
            (datetime.utcnow() - self.messages[f"{obj_id}"]) >
                timedelta(seconds=self.sub_sampling_period)):
            self.messages[f"{obj_id}"] = datetime.utcnow()
            return True
        return False

    def should_process_by_tstamp(self, message: UnifiedModel):
        """Method that determines if a message will be processed or not
        This is allowed if a new ctx_id to obj_id combination or if
        existing key sub_sampling period has elapsed. This copes with
        unordered messages"""

        try:
            obj_id = str(message.snapshot.obj)
            t_stamp = datetime.strptime(message.Hd.Ti, '%Y%m%dT%H%M%S.%f')
            if (obj_id not in self.messages or
                (t_stamp - self.messages[obj_id]) >
                    timedelta(milliseconds=self.sub_sampling_period*1000)):
                self.messages[obj_id] = t_stamp
                return True
        except ValueError:
            pass
        return False

    def clean_up_unused_entries(self):
        """Method that is executed after should_process to cleanup
        messages dictionary. It only runs every 60 seconds"""
        num_of_cleaned_up_entries = 0
        if (datetime.utcnow() - self.last_time_cleanup_was_performed >
                timedelta(seconds=60)):
            for k in list(self.messages.keys()):
                if ((datetime.utcnow() - self.messages[k]) > timedelta(
                        seconds=self.timeout_period)):
                    del self.messages[k]
                    num_of_cleaned_up_entries += 1
            self.last_time_cleanup_was_performed = datetime.utcnow()
            logging.info("Number of cleaned-up entries: "
                         f"{num_of_cleaned_up_entries}")
            logging.info("Number of object ids in sub sampler: "
                         f"{len(self.messages)}")
