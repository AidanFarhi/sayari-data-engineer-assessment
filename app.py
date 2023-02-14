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
    business_search_payload = {'SEARCH_VALUE': 'X', 'STARTS_WITH_YN': 'true', 'ACTIVE_ONLY_YN': True}
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
                endpoint, callback=self.process_business_data, 
                headers=self.headers, meta={'company': data[business_id]['TITLE'][0]}
            )

    def process_business_data(self, response):
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
        self.generate_graph(pd.read_csv('data.csv'))

    def generate_graph(self, df):
        edge_df = self.generate_edge_list(df)
        g = nx.Graph()
        g.add_nodes_from([x for col in df.columns for x in df[col].dropna().unique()])
        for _, row in edge_df.iterrows(): 
            g.add_edge(row['source'], row['target'])
        pos = nx.spring_layout(g, k=.2)
        plt.figure(figsize=(7,7)) 
        nx.draw(
            g, pos, node_size=32, font_size=2.25, font_color='white', 
            with_labels=True, width=.3, edge_color='grey'
        )
        plt.savefig('graph.png', dpi=500, facecolor='black')

    def generate_edge_list(self, df):
        cols = ['Company', 'Commercial Registered Agent', 'Registered Agent', 'Owners']
        dfs = [df[[c1, c2]][(~df[c2].isnull()) & (~df[c1].isnull())].rename(columns={c1: 'source', c2: 'target'})
            for i, c1 in enumerate(cols) for c2 in cols[i+1:]]
        return pd.concat(dfs)
