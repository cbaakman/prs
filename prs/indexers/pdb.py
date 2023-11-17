from typing import IO
from datetime import datetime
import logging

from prs.models.databank import Databank


_log = logging.getLogger(__name__)


DB_NAMES = {
    "SWS": "sprot",
    "TREMBL": "uniprot",
    "UNP": "uniprot",
}


AMINO_ACID_ONE_LETTER_CODES = {
  'ALA' : 'A',
  'ARG' : 'R',
  'ASN' : 'N',
  'ASP' : 'D',
  'CYS' : 'C',
  'GLN' : 'Q',
  'GLU' : 'E',
  'GLY' : 'G',
  'HIS' : 'H',
  'ILE' : 'I',
  'LEU' : 'L',
  'LYS' : 'K',
  'MET' : 'M',
  'PHE' : 'F',
  'PRO' : 'P',
  'SER' : 'S',
  'THR' : 'T',
  'TRP' : 'W',
  'TYR' : 'Y',
  'VAL' : 'V',
  'GLX' : 'Z',
  'ASX' : 'B',
}


def index_pdb(pdb_file: IO, pdb_atom_databank: Databank, pdb_seqres_databank: Databank):

    pdb_id = None
    atom_sequences = {}
    seqres_sequences = {}
    model_count = 0
    ref_text = ""
    remark_text = ""
    source_text = ""
    ligand_names = set([])
    for line in pdb_file:
        line_type = line[:6].strip()
        line_data = line[6:].strip()

        if line_type == "HEADER":

            pdb_id = line[62: 66]

            pdb_atom_databank.index_unique_string(pdb_id, 'id', pdb_id)
            pdb_seqres_databank.index_unique_string(pdb_id, 'id', pdb_id)

        elif line_type == "TITLE":

            pdb_atom_databank.index_text(pdb_id, 'title', line_data)
            pdb_seqres_databank.index_text(pdb_id, 'title', line_data)

        elif line_type == "MODEL":

            model_count += 1

        elif line_type == "COMPND":

            if ':' in line_data:
                i = line_data.find(':', 4)

                key = line[4: i].strip()
                value = line[i:].strip()


                key = key.strip()
                value = value.strip().rstrip(';')

                if key == "MOLECULE":
                    pdb_atom_databank.index_string(pdb_id, 'molecule', value.lower())
                    pdb_seqres_databank.index_string(pdb_id, 'molecule', value.lower())

                elif key == "EC":
                    pdb_atom_databank.index_string(pdb_id, 'ec', value)
                    pdb_seqres_databank.index_string(pdb_id, 'ec', value)

        elif line_type == "SOURCE":

            source_text += line_data[4:]

        elif line_type == "KEYWDS":

            pdb_atom_databank.index_string(pdb_id, 'keyword', line_data)
            pdb_seqres_databank.index_string(pdb_id, 'keyword', line_data)

        elif line_type == "EXPDTA":

            pdb_atom_databank.index_unique_string(pdb_id, 'method', line_data)
            pdb_seqres_databank.index_unique_string(pdb_id, 'method', line_data)

        elif line_type == "AUTHOR":

            for name in line_data[4:].split(','):
                name = name.strip()
                if len(name) > 0:
                    pdb_atom_databank.index_string(pdb_id, 'author', name)
                    pdb_seqres_databank.index_string(pdb_id, 'author', name)

        elif line_type == "REVDAT":

            date_s = line[13: 23].strip()

            if len(date_s) > 0:
                date_s = date_s.replace("JAN", '1').replace("FEB", '2').replace("MAR", '3').replace("APR", '4').replace("MAY", '5') \
                               .replace("JUN", '6').replace("JUL", '7').replace("AUG", '8').replace("SEP", '9').replace("OCT", '10') \
                               .replace("FEB", '2').replace("NOV", '11').replace("DEC", '12')

                date = datetime.strptime(date_s, "%d-%m-%y")

                pdb_atom_databank.index_date(pdb_id, 'revision', date)
                pdb_seqres_databank.index_date(pdb_id, 'revision', date)

        elif line_type == "JRNL":

            ref_text += line_data + " "

        elif line_type == "REMARK":

            remark_text += line_data[4:] + " "

        elif line_type == "DBREF":

            ids = line_data.split()
            db_id = ids[4]
            if db_id in DB_NAMES:

                db_name = DB_NAMES[db_id]
                in_db_id = ids[6]
                chain_id = ids[1]

                pdb_atom_databank.index_link(pdb_id + '.' + chain_id, db_name, in_db_id)
                pdb_seqres_databank.index_link(pdb_id + '.' + chain_id, db_name, in_db_id)
            else:
                pdb_atom_databank.index_string(pdb_id + '.' + chain_id, db_id, in_db_id)
                pdb_seqres_databank.index_string(pdb_id + '.' + chain_id, db_id, in_db_id)

        elif line_type == "HETATM" or line_type == "ATOM":

            atom_number = int(line[6: 11])
            atom_name = line[11: 16].strip()
            altloc = line[16]
            residue_name = line[17: 20].strip()
            chain_id = line[21]
            residue_number = int(line[22: 26])
            model_number_s = line[72: 75].strip()

            if len(model_number_s) > 0:
                model_number = int(model_number_s)
                if model_number != 1:
                    continue

            if residue_name != "HOH":
                if line_type == "HETATM":
                    ligand_names.add(residue_name)

                elif line_type == "ATOM" and atom_name == "CA" and altloc in (' ', 'A'):

                    if chain_id not in atom_sequences:
                        atom_sequences[chain_id] = ""

                    atom_sequences[chain_id] += AMINO_ACID_ONE_LETTER_CODES.get(residue_name, 'X')

        elif line_type == "SEQRES":

            chain_id = line[11]

            if chain_id not in seqres_sequences:
                seqres_sequences[chain_id] = ""

            for residue_name in line[19:].split():
                seqres_sequences[chain_id] += AMINO_ACID_ONE_LETTER_CODES.get(residue_name, 'X')

    for ligand_name in ligand_names:
        pdb_atom_databank.index_string(pdb_id, "ligand", ligand_name)
        pdb_seqres_databank.index_string(pdb_id, "ligand", ligand_name)

    if len(source_text) > 0:
        pdb_atom_databank.index_text(pdb_id, "source", source_text)
        pdb_seqres_databank.index_text(pdb_id, "source", source_text)

    if len(remark_text) > 0:
        pdb_atom_databank.index_text(pdb_id, "remark", remark_text)
        pdb_seqres_databank.index_text(pdb_id, "remark", remark_text)

    if len(ref_text) > 0:
        pdb_atom_databank.index_text(pdb_id, "reference", ref_text)
        pdb_seqres_databank.index_text(pdb_id, "reference", ref_text)

    if pdb_id is not None:
        for chain_id, sequence in atom_sequences.items():
            if len(sequence) > 0:
                pdb_atom_databank.set_sequence(pdb_id + '.' + chain_id, sequence)

        for chain_id, sequence in seqres_sequences.items():
            if len(sequence) > 0:
                pdb_seqres_databank.set_sequence(pdb_id + '.' + chain_id, sequence)

    pdb_atom_databank.index_number(pdb_id, "model_count", model_count)
    pdb_seqres_databank.index_number(pdb_id, "model_count", model_count)
