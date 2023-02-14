import scrapy
import json
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
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
            'Company': response.meta['company'].upper(), 'Commercial Registered Agent': None, 
            'Registered Agent': None, 'Owners': None
        }
        for r in response.json()['DRAWER_DETAIL_LIST']:
            if r['LABEL'] == 'Commercial Registered Agent': 
                new_row['Commercial Registered Agent'] = r['VALUE'][:r['VALUE'].index('\n')].upper()
            if r['LABEL'] == 'Registered Agent,': 
                new_row['Registered Agent'] = r['VALUE'].upper()
            if r['LABEL'] == 'Owner Name': 
                new_row['Owners'] = r['VALUE'].upper()
        self.company_rows.append(new_row)
     
    def closed(self, reason):
        df = pd.DataFrame(self.company_rows)
        df.to_csv('data.csv', index=False)
        self.generate_graph(df)
    
    def generate_graph(self, df):
        # Need to generate edge list of companies that are connected
        pass
        # df = pd.read_csv('data.csv')
        # commercial_registered_agent_df = df[df['Commercial Registered Agent'].notnull()]
        # registered_agent_df = df[df['Registered Agent'].notnull()]
        # owners_df = df[df['Owners'].notnull()]
        # g = nx.Graph()
        # g.add_nodes_from(df['Company'])
        # g2 = nx.from_pandas_edgelist(commercial_registered_agent_df, source='Company', target='Commercial Registered Agent')
        # g3 = nx.from_pandas_edgelist(registered_agent_df, source='Company', target='Registered Agent')
        # g4 = nx.from_pandas_edgelist(owners_df, source='Company', target='Owners')
        # combined = nx.compose(g, g2)
        # combined2 = nx.compose(combined, g3)
        # all_nodes = nx.compose(combined2, g4)
        # pos = nx.spring_layout(all_nodes, k=.2)
        # plt.figure(figsize=(9,9)) 
        # nx.draw(
        #     all_nodes, pos, node_size=32, font_size=2.25, font_color='white', 
        #     with_labels=True, width=.3, edge_color='grey'
        # )
        # plt.savefig('graph.png', dpi=500, facecolor='black')
