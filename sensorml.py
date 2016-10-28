from datetime import datetime
from urllib import urlencode
from collections import OrderedDict

import pandas as pd

from owslib.sos import SensorObservationService
from owslib.swe.sensor.sml import SensorML, Contact, Documentation
from owslib.util import testXMLValue, testXMLAttribute, nspath_eval
from owslib.namespaces import Namespaces

from pyoos.collectors.ioos.swe_sos import IoosSweSos
from pyoos.parsers.ioos.describe_sensor import IoosDescribeSensor
from pyoos.parsers.ioos.one.describe_sensor import ont


# These functions are all from OWSLib, with minor adaptations

def get_namespaces():
    n = Namespaces()
    namespaces = n.get_namespaces(["sml", "gml", "xlink", "swe"])
    namespaces["ism"] = "urn:us:gov:ic:ism:v2"
    return namespaces

namespaces = get_namespaces()


def nsp(path):
    return nspath_eval(path, namespaces)


def get_stations_df(sos_url, station_urns_sel=None):
    """ Returns a DataFrame
    """
    # LATER: ADD ERROR TEST/CATCH AFTER EACH WEB REQUEST
    oFrmt = 'text/xml; subtype="sensorML/1.0.1/profiles/ioos_sos/1.0"'

    if station_urns_sel is not None:
        params = {'service': 'SOS', 'request': 'GetCapabilities', 'acceptVersions': '1.0.0'}
        sosgc = SensorObservationService(sos_url + '?' + urlencode(params))
        station_urns = station_urns_sel
    else:
        sos_collector = IoosSweSos(sos_url)
        station_urns = [urn.name for urn in sos_collector.server.offerings
                        if 'network' not in urn.name.split(':')]
        sos_collector.features = station_urns
        sml_lst = sos_collector.metadata(timeout=200)

    station_recs = []
    for station_idx, station_urn in enumerate(station_urns):
        if station_urns_sel is not None:
            sml_str = sosgc.describe_sensor(procedure=station_urn, outputFormat=oFrmt,
                                            timeout=200)
            sml = SensorML(sml_str)
        else:
            sml = sml_lst[station_idx]

        ds = IoosDescribeSensor(sml._root)

        pos = testXMLValue(ds.system.location.find(nsp('gml:Point/gml:pos')))

        system_el = sml._root.findall(nsp('sml:member'))[0].find(nsp('sml:System'))

        # Assume there's a single DocumentList/member; will read the first one only.
        # Assume that member corresponds to xlink:arcrole="urn:ogc:def:role:webPage"
        documents = system_el.findall(nsp('sml:documentation/sml:DocumentList/sml:member'))
        if len(documents) > 0:
            document = Documentation(documents[0])
            webpage_url = document.documents[0].url
        else:
            webpage_url = None

        contacts = system_el.findall(nsp('sml:contact/sml:ContactList/sml:member'))
        contacts_dct = {}
        for c in contacts:
            contact = Contact(c)
            role = contact.role.split('/')[-1]
            contacts_dct[role] = contact

        sweQuants = system_el.findall(nsp('sml:outputs/sml:OutputList/sml:output/swe:Quantity'))
        quant_lst = [sweQuant.attrib['definition'] for sweQuant in sweQuants]
        parameter_lst = [sweQuant.split('/')[-1] for sweQuant in quant_lst]

        station = OrderedDict()
        station['station_urn'] = station_urn
        station['lon'] = float(pos.split()[1])
        station['lat'] = float(pos.split()[0])

        station['shortName'] = ds.shortName
        station['longName'] = ds.longName
        station['wmoID'] = ds.get_ioos_def('wmoID', 'identifier', ont)

        # Beware that a station can have >1 classifier of the same type
        # This code does not accommodate that possibility
        station['platformType'] = ds.platformType
        station['parentNetwork'] = ds.get_ioos_def('parentNetwork', 'classifier', ont)
        station['sponsor'] = ds.get_ioos_def('sponsor', 'classifier', ont)
        station['webpage_url'] = webpage_url

        station['operatorSector'] = ds.get_ioos_def('operatorSector', 'classifier', ont)
        station['operator_org'] = contacts_dct['operator'].organization
        station['operator_country'] = contacts_dct['operator'].country
        station['operator_url'] = contacts_dct['operator'].url
        # pull out email address(es) too?
        # station_dct['operator_email'] = contacts_dct['operator'].electronicMailAddress

        station['publisher'] = ds.get_ioos_def('publisher', 'classifier', ont)
        station['publisher_org'] = contacts_dct['publisher'].organization
        station['publisher_url'] = contacts_dct['publisher'].url
        # station_dct['publisher_email'] = contacts_dct['publisher'].electronicMailAddress

        station['starting'] = ds.starting
        station['ending'] = ds.ending
        station['starting_isostr'] = datetime.isoformat(ds.starting)
        station['ending_isostr'] = datetime.isoformat(ds.ending)

        station['parameter_uris'] = ','.join(quant_lst)
        station['parameters'] = ','.join(parameter_lst)

        station_recs.append(station)

    stations_df = pd.DataFrame.from_records(station_recs, columns=station.keys())
    stations_df.index = stations_df['station_urn']

    return stations_df
