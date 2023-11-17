from typing import IO
from datetime import datetime
import logging

from pdbecif.mmcif_tools import MMCIF2Dict

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


def index_mmcif(gz_path: str, atom_databank: Databank, seqres_databank: Databank):

    for pdb_id, mmcif_dict in MMCIF2Dict().parse(gz_path).items():

        pdb_id = pdb_id.lower()

        model_ids = set([])
        ligand_names = set([])
        atom_sequences = {}
        seqres_sequences = {}

        if '_struct_keywords' in mmcif_dict:
            if 'entry_id' in mmcif_dict['_struct_keywords']:

                entry_id = mmcif_dict['_struct_keywords']['entry_id'].lower()

                atom_databank.index_unique_string(pdb_id, 'id', entry_id)
                seqres_databank.index_unique_string(pdb_id, 'id', entry_id)

            if 'pdbx_keywords' in mmcif_dict['_struct_keywords']:

                text = mmcif_dict['_struct_keywords']['pdbx_keywords']

                atom_databank.index_text(pdb_id, 'keywords', text)
                seqres_databank.index_text(pdb_id, 'keywords', text)

            if 'text' in mmcif_dict['_struct_keywords']:

                text = mmcif_dict['_struct_keywords']['text']

                atom_databank.index_text(pdb_id, 'keywords', text)
                seqres_databank.index_text(pdb_id, 'keywords', text)

        elif '_entry' in mmcif_dict and 'id' in mmcif_dict['_entry']:

            entry_id = mmcif_dict['_entry']['id'].lower()

            atom_databank.index_unique_string(pdb_id, 'id', entry_id)
            seqres_databank.index_unique_string(pdb_id, 'id', entry_id)

        if '_exptl' in mmcif_dict:
            if type(mmcif_dict['_exptl']['method']) == str:

                method = mmcif_dict['_exptl']['method']

                atom_databank.index_string(pdb_id, 'method', method)
                seqres_databank.index_string(pdb_id, 'method', method)
            else:
                for method in mmcif_dict['_exptl']['method']:
                    atom_databank.index_string(pdb_id, 'method', method)
                    seqres_databank.index_string(pdb_id, 'method', method)

        if '_refine' in mmcif_dict:
            if type(mmcif_dict['_refine']['ls_d_res_high']) == str and mmcif_dict['_refine']['ls_d_res_high'] != '.':

                resolution = float(mmcif_dict['_refine']['ls_d_res_high'])

                atom_databank.index_number(pdb_id, 'resolution', resolution)
                seqres_databank.index_number(pdb_id, 'resolution', resolution)
            else:
                for s in mmcif_dict['_refine']['ls_d_res_high']:
                    if s != '.':
                        resolution = float(s)

                        atom_databank.index_number(pdb_id, 'resolution', resolution)
                        seqres_databank.index_number(pdb_id, 'resolution', resolution)

        atoms = mmcif_dict['_atom_site']
        for atom_index, atom_id in enumerate(atoms['id']):
            atom_group = atoms['group_PDB'][atom_index]
            model_number = atoms['pdbx_PDB_model_num'][atom_index]
            residue_name = atoms['auth_comp_id'][atom_index]
            chain_id = atoms['label_asym_id'][atom_index]
            atom_name = atoms['auth_atom_id'][atom_index]
            alt_id = atoms['label_alt_id'][atom_index]

            model_ids.add(model_number)

            if atom_group == "ATOM" and model_number == '1' and residue_name != "HOH" and atom_name == 'CA' and alt_id in ['.', 'A']:
                if chain_id not in atom_sequences:
                    atom_sequences[chain_id] = ""

                atom_sequences[chain_id] += AMINO_ACID_ONE_LETTER_CODES.get(residue_name, 'X')

            elif atom_group == "HETATM" and residue_name != "HOH":
                ligand_names.add(residue_name)

        if '_struct' in mmcif_dict:
            if 'entry_id' in mmcif_dict['_struct']:
                entry_id = mmcif_dict['_struct']['entry_id'].lower()

                atom_databank.index_unique_string(pdb_id, 'id', entry_id)
                seqres_databank.index_unique_string(pdb_id, 'id', entry_id)

            if 'title' in mmcif_dict['_struct']:
                title = mmcif_dict['_struct']['title']

                atom_databank.index_text(pdb_id, 'title', title)
                seqres_databank.index_text(pdb_id, 'title', title)

        if '_entity' in mmcif_dict:
            for entity_index, entity_id in enumerate(mmcif_dict['_entity']['id']):
                type_ = mmcif_dict['_entity']['type'][entity_index]
                description = mmcif_dict['_entity']['pdbx_description'][entity_index]
                ec = mmcif_dict['_entity']['pdbx_ec'][entity_index]
                mutation = mmcif_dict['_entity']['pdbx_mutation'][entity_index]
                fragment = mmcif_dict['_entity']['pdbx_fragment'][entity_index]
                details = mmcif_dict['_entity']['details'][entity_index]

                if type_ != "polymer":
                    continue

                if description != '?':
                    atom_databank.index_text(pdb_id, 'molecule', description)
                    seqres_databank.index_text(pdb_id, 'molecule', description)

                if fragment != '?':
                    atom_databank.index_text(pdb_id, 'molecule', fragment)
                    seqres_databank.index_text(pdb_id, 'molecule', fragment)

                if details != '?':
                    atom_databank.index_text(pdb_id, 'molecule', details)
                    seqres_databank.index_text(pdb_id, 'molecule', details)

                if ec != '?':
                    atom_databank.index_string(pdb_id, 'ec', ec)
                    seqres_databank.index_string(pdb_id, 'ec', ec)

                if mutation != '?':
                    atom_databank.index_text(pdb_id, 'mutation', mutation)
                    seqres_databank.index_text(pdb_id, 'mutation', mutation)

        if '_entity_name_com' in mmcif_dict:
            for entity_index, entity_id in enumerate(mmcif_dict['_entity_name_com']['entity_id']):
                name = mmcif_dict['_entity_name_com']['name']

                atom_databank.index_text(pdb_id, 'molecule', name)
                seqres_databank.index_text(pdb_id, 'molecule', name)

        if '_entity_poly' in mmcif_dict:
            for entity_index, entity_id in enumerate(mmcif_dict['_entity_poly']['entity_id']):
                type_ = mmcif_dict['_entity_poly']['type'][entity_index]

                atom_databank.index_text(pdb_id, 'molecule', type_)
                seqres_databank.index_text(pdb_id, 'molecule', type_)

        if '_audit_author' in mmcif_dict:
            for author_name in mmcif_dict['_audit_author']['name']:
                atom_databank.index_string(pdb_id, "author", author_name)
                seqres_databank.index_string(pdb_id, "author", author_name)

        if '_citation' in mmcif_dict:
            if type(mmcif_dict['_citation']['id']) == str:

                for key in mmcif_dict['_citation']:
                    if len(key) >= 3:
                        value = mmcif_dict['_citation'][key]

                        atom_databank.index_text(pdb_id, "reference", value)
                        seqres_databank.index_text(pdb_id, "reference", value)
            else:
                for publication_index, publication_id in enumerate(mmcif_dict['_citation']['id']):
                    if publication_id != '?':
                        for key in mmcif_dict['_citation']:
                            value = mmcif_dict['_citation'][key][publication_index]

                            atom_databank.index_text(pdb_id, "reference", value)
                            seqres_databank.index_text(pdb_id, "reference", value)

        if '_struct_ref' in mmcif_dict:
            for ref_index, ref_id in enumerate(mmcif_dict['_struct_ref']['id']):
                db_id = mmcif_dict['_struct_ref']['db_name'][ref_index]
                in_db_id = mmcif_dict['_struct_ref']['db_code'][ref_index]

                if db_id in DB_NAMES:
                    db_name = DB_NAMES[db_id]

                    atom_databank.index_link(pdb_id, db_name, in_db_id)
                    seqres_databank.index_link(pdb_id, db_name, in_db_id)
                else:
                    atom_databank.index_string(pdb_id, db_id, in_db_id)
                    seqres_databank.index_string(pdb_id, db_id, in_db_id)

        if '_pdbx_poly_seq_scheme' in mmcif_dict:
            for res_index, res_id in enumerate(mmcif_dict['_pdbx_poly_seq_scheme']['seq_id']):
                chain_id = mmcif_dict['_pdbx_poly_seq_scheme']['asym_id'][res_index]
                three_letter_code = mmcif_dict['_pdbx_poly_seq_scheme']['mon_id'][res_index]
                one_letter_code = AMINO_ACID_ONE_LETTER_CODES.get(three_letter_code, 'X')

                if chain_id not in seqres_sequences:
                    seqres_sequences[chain_id] = ""

                seqres_sequences[chain_id] += one_letter_code

        for ligand_name in ligand_names:
            atom_databank.index_string(pdb_id, "ligand", ligand_name)
            seqres_databank.index_string(pdb_id, "ligand", ligand_name)

        for chain_id, sequence in atom_sequences.items():
            if len(sequence.replace("X", "")) > 0:
                atom_databank.set_sequence(pdb_id + '.' + chain_id, sequence)

        for chain_id, sequence in seqres_sequences.items():
            if len(sequence.replace("X", "")) > 0:
                seqres_databank.set_sequence(pdb_id + '.' + chain_id, sequence)

        model_count = len(model_ids)
        atom_databank.index_number(pdb_id, "model_count", model_count)
        seqres_databank.index_number(pdb_id, "model_count", model_count)
