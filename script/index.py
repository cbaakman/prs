import sys
import os
import logging
from glob import glob

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_path)
from prs.indexers.uniprot import index_uniprot
from prs.indexers.mmcif import index_mmcif
from prs.models.databank import Databank


if __name__ == "__main__":

    data_path = sys.argv[1]

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    with Databank("sprot") as db:
        with open(os.path.join(data_path, "uniprot/uniprot_sprot.dat"), 'rt') as f:
            index_uniprot(f, db)

    with Databank("uniprot") as db:
        with open(os.path.join(data_path, "uniprot/uniprot_sprot.dat"), 'rt') as f:
            index_uniprot(f, db)

        with open(os.path.join(data_path, "uniprot/uniprot_trembl.dat"), 'rt') as f:
            index_uniprot(f, db)

    with Databank("pdb_atom") as atom_database:
        with Databank("pdb_seqres") as seqres_database:
            for cif_path in glob(os.path.join(data_path, "mmCIF/????.cif.gz")):
                index_mmcif(cif_path, atom_database, seqres_database)

