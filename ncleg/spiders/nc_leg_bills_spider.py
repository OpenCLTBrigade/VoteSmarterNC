import scrapy
from ncleg.items import Bill
from urllib.parse import urlparse, parse_qs

class NcLegBillsSpider(scrapy.Spider):
    # Spider name
    name = "bills"


    def __init__(self, chamber='', session='2017', number='', *args, **kwargs):
        super(NcLegBillsSpider, self).__init__(*args, **kwargs)

        # Check if parsing single chamber or both
        chamber_prefixes = ['H', 'S']
        if chamber in chamber_prefixes:
            self.chambers = [chamber]
        else:
            self.chambers = chamber_prefixes

        self.session = session

        # Remove all whitespace in number parameter then split commas into array
        bill_numbers = number.replace(" ", "")
        if bill_numbers:
            self.billList = bill_numbers.split(",")
        else:
            self.billList = None


    def start_requests(self):
        # Bills URL skeleton
        # Bills are numbered predictably so increment bill number += 1
        for chamber in self.chambers:
            # Start with first bill for each chamber
            self.isBillsEnd = False

            bills_url = ('https://www.ncleg.net/gascripts/BillLookUp/BillLookUp.pl'
                              f'?BillID={chamber}{{bill}}&Session={self.session}')

            if self.billList:
                for bill_number in self.billList:
                    url = bills_url.format(bill=bill_number)
                    yield scrapy.Request(url=url, callback=self.parse)
            else:
                bill_number = 1
                while not self.isBillsEnd:
                    url = bills_url.format(bill=bill_number)
                    yield scrapy.Request(url=url, callback=self.parse)
                    bill_number += 1

 
    def parse(self, response):
        # Return when we have incremented past the last known bill
        if len(response.xpath('//div[@id = "title"]/text()').re('Not Found')) > 0:
            chamber = parse_qs(urlparse(response.url).query)['BillID'][0][0]
            self.isBillsEnd = True
            return

        # Use Bill Item to catch data
        item = Bill()
        item['number'] = response.xpath('//div/table/tr/td/table/tr/td[2]').re('\d+')[1]
        item['chamber'] = response.xpath('//div/table/tr/td/table/tr/td[2]/text()').re('\w+')[0]
        item['session'] = response.css('.titleSub::text').extract_first()
        item['title'] = response.xpath('//div[@id = "title"]/a/text()').extract_first()
        item['counties'] = response.xpath('//div/table/tr/td[1]/table[2]/tr/td[3]/table/tr[4]/td/text()').re('[^,]+')
        item['statutes'] = response.xpath('/html/body/div/table/tr/td[1]/table[2]/tr/td[3]/table/tr[5]/td/div/text()').re('[^\n][^,]+')
        keywords = response.xpath('/html/body/div/table/tr/td[1]/table[2]/tr/td[3]/table/tr[6]/td/div/text()').extract_first().split(', ')
        item['keywords'] = keywords
        item['passed_House'] = False
        item['passed_Senate'] = False

        # This will loop through all possible xpaths in the detailed table, and will stop
        # if empty list is returned
        i = 3
        while (response.xpath('/html/body/div/table/tr/td[1]/center/table/tr[' + str(i) + ']/td[3]/text()').extract()):
            v = response.xpath('/html/body/div/table/tr/td[1]/center/table/tr[' + str(i) + ']/td[3]/text()').extract()
            if ('Passed 3rd Reading' in v[0] and 'House' in response.xpath('/html/body/div/table/tr/td[1]/center/table/tr[' + str(i) + ']/td[2]/text()').extract()[0]):
                item['passed_House'] = True
            if ('Passed 3rd Reading' in v[0] and 'Senate' in response.xpath('/html/body/div/table/tr/td[1]/center/table/tr[' + str(i) + ']/td[2]/text()').extract()[0]):
                item['passed_Senate'] = True
            i = i + 1

        # Check to see if bill had been ratified. This info is available in bill keywords
        item['is_ratified'] = self.isRatified(keywords)

        # Check to see if bill had been ratified and/or is law
        d_arr = response.xpath('/html/body/div/table/tr/td/table[2]/tr/td[1]/table/tr//td[1]//a/text()').extract()
        item['is_law'] = self.isLaw(d_arr)

        # In 2017 member names are embedded in links
        if (self.session == '2017'):
            item['sponsors'] = response.xpath('/html/body/div/table/tr/td[1]/table[2]/tr/td[3]/table/tr[2]/td/a/text()').re('[^,]+')
            item['sponsors_ids'] = response.xpath('/html/body/div/table/tr/td[1]/table[2]/tr/td[3]/table/tr[2]/td/a/@href').re('\d+')
            item['primary_sponsors'] = response.xpath('/html/body/div/table/tr/td[1]/table[2]/tr/td[3]/table/tr[2]/td/br/preceding-sibling::a/text()').extract()
            item['primary_sponsors_ids'] = response.xpath('/html/body/div/table/tr/td[1]/table[2]/tr/td[3]/table/tr[2]/td/br/preceding-sibling::a/@href').re('\d+')
        else:
            sponsors = response.xpath('/html/body/div/table/tr/td[1]/table[2]/tr/td[3]/table/tr[2]/td/text()').re('(?!Primary$)\w+\.?\ ?\-?\'?\w+')
            primary = sponsors.index("Primary")
            if (primary > -1):
                item['primary_sponsors'] = sponsors[0:primary]
                del sponsors[primary]
            item['sponsors'] = sponsors
        yield item


    def isRatified(self, bill_keywords):
        return "RATIFIED" in bill_keywords


    def isLaw(self, arr):
        for item in arr:
            if "Law" in item:
                return True
        return False
