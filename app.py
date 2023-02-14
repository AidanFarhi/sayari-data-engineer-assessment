import scrapy
import json
import pandas as pd
from time import sleep

class BusinessSearchSpider(scrapy.Spider):

    name = 'business_search_spider'
    headers = {
        'Content-Type': 'application/json', 'Accept': '*/*', 'authorization': 'undefined'
    }
    business_search_payload = {"SEARCH_VALUE": "X", "STARTS_WITH_YN": "true" ,"ACTIVE_ONLY_YN": True}
    business_search_endpoint = 'https://firststop.sos.nd.gov/api/Records/businesssearch'
    filing_detail_base_url = 'https://firststop.sos.nd.gov/api/FilingDetail/business/'
    company_rows = []

    def start_requests(self):
        yield scrapy.Request(
            self.business_search_endpoint, callback=self.process_search_results, method='POST',
            headers=self.headers, body=json.dumps(self.business_search_payload)
        )

    def process_search_results(self, response):
        data = response.json()['rows']
        for business_id in data:
            endpoint = f'{self.filing_detail_base_url}{business_id}/false'
            yield scrapy.Request(
                endpoint, callback=self.process_company_data, 
                headers=self.headers, meta={'company': data[business_id]['TITLE'][0]}
            )

    def process_company_data(self, response):
        sleep(.1)
        new_row = {
            'Company': response.meta['company'], 'Commercial Registered Agent': None, 
            'Registered Agent': None, 'Owner': None
        }
        for r in response.json()['DRAWER_DETAIL_LIST']:
            if r['LABEL'] == 'Commercial Registered Agent': new_row['Commercial Registered Agent'] = r['VALUE'].replace('\n', ' ')
            if r['LABEL'] == 'Registered Agent,': new_row['Registered Agent'] = r['VALUE']
            if r['LABEL'] == 'Owner Name': new_row['Owner'] = r['VALUE']
        self.company_rows.append(new_row)
     
    def closed(self, reason):
        df = pd.DataFrame(self.company_rows)
        df.to_csv('data.csv', index=False)
        print(df.head(20))