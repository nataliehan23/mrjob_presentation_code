#! /usr/bin/env python
from __future__ import division

from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol 


class CalculateCTR(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json_value'

    def mapper(self, _, ad_event):
        if ad_event['billable_impression']:
            ad_campaign = ad_event['impression']['candidate_id']
            ad_type = ad_event['impression']['ad_type']
            user_clicked_through = ad_event['billable_click']
            # emits ((123434, "SEARCH"/ "DISPLAY"), True/False) 
            yield (ad_campaign, ad_type), user_clicked
            # emits ((123434, "TOTAL"), True/False) 
            yield (ad_campaign, 'TOTAL'), user_clicked
            # emits (("TOTAL", "SEARCH"/"DISPLAY"), True/False) 
            yield ('TOTAL', ad_type), user_clicked
            # emits ("TOTAL", True/False) 
            yield 'TOTAL', user_clicked

    def reducer(self, grouping, events):
        impression_count = 0
        click_count = 0
        for event in events:
            impression_count += 1
            if event:  # i.e., it was clicked
                click_count += 1

        ctr = click_count / impression_count

        if ctr > 0:
            yield grouping, ctr


if __name__ == '__main__':
    CalculateCTR.run()
