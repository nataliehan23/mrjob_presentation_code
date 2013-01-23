#! /usr/bin/env python
from __future__ import division

from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol 


class CalculateGlobalCTR(MRJob):
    DEFAULT_INPUT_PROTOCOL = JSONProtocol

    def mapper(self, _, ad_event):
        if ad_event['billable_impression']:
            user_clicked = ad_event['billable_click']
            # emits ("TOTAL", True/False)
            yield "TOTAL", user_clicked

    def reducer(self, _, events):
        # events - (True, False, ....)
        impression_count = 0
        click_count = 0
        for event in events:
            impression_count += 1
            if event:  # i.e., it was clicked
                click_count += 1

        yield "TOTAL", click_count / impression_count


if __name__ == '__main__':
    CalculateCTR.run()
