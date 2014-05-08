#!/usr/bin/env python

import csv
import json
from sets import Set

from dateutil.parser import *
from django.contrib.gis.gdal import DataSource

from etc.gdocs import GoogleDoc

def write_aggregates():
    """
    Prepares an aggregates file for each county.
    Only counties with at least one visit will be aggregated
    because we build the county list off of the spreadsheet of
    visits.
    """

    fieldnames = [
        'name',
        'state_fips',
        'county_fips',
        'combined_fips',
        'total_visits',
        'total_fundraising_visits',
        'total_message_visits',
        'visits_2009',
        'visits_2010',
        'visits_2011',
        'visits_2012',
        'visits_2013',
        'visits_2014',
        'fundraising_2009',
        'fundraising_2010',
        'fundraising_2011',
        'fundraising_2012',
        'fundraising_2013',
        'fundraising_2014',
        'message_2009',
        'message_2010',
        'message_2011',
        'message_2012',
        'message_2013',
        'message_2014',
        '2012_obama_vote_share',
        '2012_obama_vote_pct',
        '2008_obama_vote_share',
        '2008_obama_vote_pct'
    ]

    with open('data/visits-annotated.csv', 'rb') as readfile:
        visits = list(csv.DictReader(readfile))

    counties = Set([])
    for visit in visits:
        county_json = {}
        county_json['state_fips'] = visit['state_fips'].zfill(2)
        county_json['county_fips'] = visit['county_fips'].zfill(3)
        county_json['combined_fips'] = visit['combined_fips'].zfill(5)
        county_json['name'] = visit['county']

        counties.add(json.dumps(county_json))

    with open('data/county-aggregates.csv', 'wb') as writefile:
        csvwriter = csv.DictWriter(writefile, fieldnames=fieldnames)
        csvwriter.writeheader()
        for county in counties:

            payload = json.loads(county)

            for key, value in payload.items():
                if value:
                    payload[key] = value.encode('utf-8')
                else:
                    payload[key] = None

            payload['total_visits'] = 0
            payload['total_fundraising_visits'] = 0
            payload['total_message_visits'] = 0
            payload['visits_2009'] = 0
            payload['visits_2010'] = 0
            payload['visits_2011'] = 0
            payload['visits_2012'] = 0
            payload['visits_2013'] = 0
            payload['visits_2014'] = 0
            payload['fundraising_2009'] = 0
            payload['fundraising_2010'] = 0
            payload['fundraising_2011'] = 0
            payload['fundraising_2012'] = 0
            payload['fundraising_2013'] = 0
            payload['fundraising_2014'] = 0
            payload['message_2009'] = 0
            payload['message_2010'] = 0
            payload['message_2011'] = 0
            payload['message_2012'] = 0
            payload['message_2013'] = 0
            payload['message_2014'] = 0
            payload['2008_obama_vote_share'] = 0.0
            payload['2008_obama_vote_pct'] = 0.0
            payload['2012_obama_vote_share'] = 0.0
            payload['2012_obama_vote_pct'] = 0.0

            for visit in visits:
                if visit['combined_fips'].zfill(5) == payload['combined_fips'].zfill(5):
                    payload['total_visits'] += 1

                    payload['2008_obama_vote_share'] = visit['2008_obama_vote_share']
                    payload['2008_obama_vote_pct'] = visit['2008_obama_vote_pct']
                    payload['2012_obama_vote_share'] = visit['2012_obama_vote_share']
                    payload['2012_obama_vote_pct'] = visit['2012_obama_vote_pct']

                    try:
                        visit['date'] = parse(visit['date'])
                    except AttributeError:
                        pass

                    if visit['visit_type'].lower() == "fundraiser":
                        payload['total_fundraising_visits'] += 1
                        payload['fundraising_%s' % visit['date'].year] += 1
                    else:
                        payload['total_message_visits'] += 1
                        payload['message_%s' % visit['date'].year] += 1

                    payload['visits_%s' % visit['date'].year] +=1

            csvwriter.writerow(payload)


def write_visit_file():
    fieldnames = ['date', 'location', 'city', 'state', 'county', 'state_fips', 'county_fips', 'combined_fips', 'visit_type', 'description', 'link']

    with open('data/visits.csv', 'wb') as writefile:
        csvwriter = csv.DictWriter(writefile, fieldnames=fieldnames)
        csvwriter.writeheader()
        for visit in Visit.objects.all():
            csvwriter.writerow(visit.to_dict())

def write_annotations():
    with open('data/visits.csv', 'rb') as readfile:
        visits = list(csv.DictReader(readfile))

    ds2012 = DataSource('www/assets/shp/elpo12p010g.shp')
    layer2012 = ds2012[0]

    ds2008 = DataSource('www/assets/shp/elpo08p020.shp')
    layer2008 = ds2008[0]

    fieldnames = ['date', 'location', 'city', 'state', 'county', '2012_obama_vote_share', '2012_obama_vote_pct', '2008_obama_vote_share', '2008_obama_vote_pct', 'state_fips', 'county_fips', 'combined_fips', 'visit_type', 'description', 'link']

    with open('data/visits-annotated.csv', 'wb') as writefile:
        csvwriter = csv.DictWriter(writefile, fieldnames=fieldnames)
        csvwriter.writeheader()
        for i, visit in enumerate(visits):

            unknown = True

            print "START %s" % i

            for county in layer2008:
                if visit['combined_fips'].zfill(5) == county['FIPS'].as_string():
                    visit['2008_obama_vote_share'] = float(county['PERCENT_DE'].as_string()) - float(county['PERCENT_RE'].as_string())
                    visit['2008_obama_vote_pct'] = float(county['PERCENT_DE'].as_string())
                    print "\tGot 2008 fips."
                    unknown = False
                    break

                elif (visit['state'].lower() == county['STATE'].as_string().lower()) and (visit['county'].lower() == county['COUNTY'].as_string().lower()):
                    visit['2008_obama_vote_share'] = float(county['PERCENT_DE'].as_string()) - float(county['PERCENT_RE'].as_string())
                    visit['2008_obama_vote_pct'] = float(county['PERCENT_DE'].as_string())
                    print "\tGot 2008 state/county name."
                    unknown = False
                    break

            if unknown:
                print '\t!! Failed to lookup 2008 results for fips: %s' % visit['combined_fips'].zfill(5)

            unknown = True

            for county in layer2012:
                if visit['combined_fips'].zfill(5) == county['FIPS'].as_string():
                    visit['2012_obama_vote_share'] = float(county['PCT_OBM'].as_string()) - float(county['PCT_ROM'].as_string())
                    visit['2012_obama_vote_pct'] = float(county['PCT_OBM'].as_string())
                    print "\tGot 2012 fips."
                    unknown = False
                    break

                elif (visit['state'].lower() == county['STATE'].as_string().lower()) and (visit['county'].lower() == county['COUNTY'].as_string().lower()):
                    visit['2012_obama_vote_share'] = float(county['PCT_OBM'].as_string()) - float(county['PCT_ROM'].as_string())
                    visit['2012_obama_vote_pct'] = float(county['PCT_OBM'].as_string())
                    print "\tGot 2012 state/county name."
                    unknown = False
                    break

            if unknown:
                print '\t!! Failed to lookup 2012 results for fips: %s' % visit['combined_fips'].zfill(5)

            print "FINISH %s" % i
            csvwriter.writerow(visit)

def download_csv():
    doc = {
        "key": "0AgtV5am-X0b8dG9qS21LQUNMSDNJakRFNkpvbFBGbVE",
        "file_name": "visits",
        "file_format": "csv",
        "gid": "8"
    }
    g = GoogleDoc(**doc)
    g.get_auth()
    g.get_document()