#! /usr/bin/env python
from __future__ import division

from mrjob.job import MRJob

class CalculateCTR(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json_value'

    def mapper(self, _, ad_event):
        if ad_event['billable_impression']:
            user = ad_event['opportunity']['unique_visitor_id']
            ad_timestamp = ad_event['opportunity']['ad_delivery_end_time']
            user_clicked = ad_event['billable_click']
            # emits (1234, (23423534, True/False))
            # 1234 - user id
            # 23423534 - timestamp
            yield user, (ad_timestamp, user_clicked)

    def reducer(self, user, events):
        i = 0
        for _, user_clicked in sorted(events):
            # emits (0/1/2/3...., True/False)
            # 0/1/2/3... position           
            yield i, user_clicked
            i += 1


if __name__ == '__main__':
    CalculateCTR.run()
