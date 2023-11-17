from typing import IO
import re
from datetime import datetime
import logging

from prs.models.databank import Databank


_log = logging.getLogger(__name__)


EC_PATTERN = re.compile(r'EC\=([0-9\-]+\.[0-9\-]+\.[0-9\-]+\.[0-9\-]+)')


def _index_description(description: str, databank: Databank, entry_id: int):

    key = None
    for line in description.split('; '):
        line = line.strip()
        if ':' in line:
            i = line.find(':')
            key = line[:i].strip()
            value = line[i + 1:].strip()
        else:
            value = line

        value = value.strip()

        if key == 'RecName' and value.startswith('Full='):
            databank.index_string(entry_id, "title", value[5:])

    databank.index_text(entry_id, 'de', description)

    for ec_match in EC_PATTERN.finditer(description):
        ec_code = ec_match.group(1)
        databank.index_string(entry_id, "ec", ec_code)


def _index_comment(text: str, databank: Databank, entry_id: int):

    lines = text.split('\n')

    for index in range(len(lines) - 4):
        if len(lines[index].replace('-', '').strip()) == 0 and \
           lines[index + 1].strip() == "Copyrighted by the UniProt Consortium, see https://www.uniprot.org/terms" and \
           lines[index + 2].strip() == "Distributed under the Creative Commons Attribution (CC BY 4.0) License" and \
           len(lines[index + 3].replace('-', '').strip()) == 0:

            lines = lines[:index] + lines[index + 4:]
            break

    text = '\n'.join(lines)

    databank.index_text(entry_id, 'cc', text)


def index_uniprot(file_: IO, databank: Databank):

    entry_id = None
    description = ""
    comment = ""
    sequence = ""
    gene_names = ""
    for line in file_:

        data_type = line[:5].strip()
        data = line[5:].rstrip('\n')

        if data_type == 'ID':
            description = ""
            comment = ""
            gene_names = ""

            entry_id = data.split()[0]

            databank.index_unique_string(entry_id, 'id', entry_id)

        elif data_type == 'AC':

            for ac in [s.strip() for s in data.split('; ')]:
                databank.index_string(entry_id, 'ac', ac)

        elif data_type == 'DE':
            description += data + '\n'

        elif data_type == 'DT':
            i = data.find(',')

            s = data[:i].replace("JAN", '1').replace("FEB", '2').replace("MAR", '3').replace("APR", '4').replace("MAY", '5') \
                        .replace("JUN", '6').replace("JUL", '7').replace("AUG", '8').replace("SEP", '9').replace("OCT", '10') \
                        .replace("FEB", '2').replace("NOV", '11').replace("DEC", '12')

            date = datetime.strptime(s, "%d-%m-%Y")
            event = data[i + 1:].rstrip('.')

            databank.index_date(entry_id, 'dt', date)

        elif data_type == 'GN':
            gene_names += data + ' '

        elif data_type in ['OC', 'KW']:
            for keyword in data.strip(';').strip('.').split('; '):
                databank.index_string(entry_id, data_type.lower(), keyword)

        elif data_type == 'CC':
            comment += data + '\n'

        elif data_type == 'RX':
            for statement in data.rstrip(';').split('; '):
                key, value = statement.split('=')

                databank.index_string(entry_id, key.lower(), value)

        elif data_type.startswith('R'):
            databank.index_string(entry_id, 'ref', data)

        elif data_type == 'DR':

            databank.index_text(entry_id, 'dr', data)

        elif data_type == 'SQ':

            for statement in data.rstrip(';').split('; '):
                if statement.startswith("SEQUENCE ") and statement.endswith(" AA"):
                    length = int(statement[9: -3])
                    databank.index_number(entry_id, 'length', length)

                elif statement.endswith(" MW"):
                    mw = int(statement[:-3])
                    databank.index_number(entry_id, 'mw', mw)

                elif statement.endswith(" CRC64"):
                    cdc64 = statement[:-6].strip()
                    databank.index_string(entry_id, 'cdc64', cdc64)

            sequence = ""

        elif data_type == '':
            sequence += data.replace(' ', '').strip()

        elif data_type == '//':
            if len(description) > 0:
                _index_description(description, databank, entry_id)

            if len(comment) > 0:
                _index_comment(comment, databank, entry_id)

            if len(sequence) > 0:
                databank.set_sequence(entry_id, sequence)

            if len(gene_names) > 0:
                for statement in gene_names.rstrip('; ').split('; '):
                    key, value = statement.split('=')
                    for name in value.split(','):
                        databank.index_string(entry_id, 'gn', name)

        elif data_type != 'XX':
            databank.index_text(entry_id, data_type.lower(), data)
