from Common.prefixes import *

# these are predicates from DGIDB as well as drug and chemical activity types from drug central
DGIDB_PREDICATE_MAPPING = {
    "ac50": f"{DGIDB}:activator",
    "activator": f"{DGIDB}:activator",
    "agonist": f"{DGIDB}:agonist",
    "allosteric_antagonist": f"{DGIDB}:antagonist",
    "allosteric_modulator": f"{DGIDB}:modulator",
    "antagonist": f"{DGIDB}:antagonist",
    "antibody": f"RO:0002436",
    "antibody_binding": f"RO:0002436",
    "antisense_inhibitor": f"{DGIDB}:inhibitor",
    "app_ki": f"RO:0002434",  # apparent Ki?  if so change to RO:0002436
    "app_km": f"RO:0002434",  # apperent Km?  if so change to RO:0002436
    "binding_agent": f"RO:0002436",
    "blocker": f"{DGIDB}:blocker",
    "channel_blocker": f"{DGIDB}:channel_blocker",
    "ec50": f"{DGIDB}:agonist",
    "ed50": f"RO:0002434",  # Effective Dose. Where does this predicate come from? CB (2024_07): "it makes no sense to have an ed50 between a chemical and a gene/protein"
    "gating_inhibitor": f"{DGIDB}:gating_inhibitor",
    "gi50": f"{DGIDB}:Inhibitor",  # Growth Inhibitor
    "ic50": f"{DGIDB}:inhibitor",
    "inhibitor": f"{DGIDB}:inhibitor",
    "interacts_with": f"RO:0002434",  # Where does this predicate come from? Possiblely needs to be modified to RO:0002436
    "inverse_agonist": f"{DGIDB}:inverse_agonist",
    "ka": f"RO:0002436",
    "kact": f"RO:0002436",  # is this a miss type of kcat?
    "kb": f"RO:0002436",  # {DGIDB}:binder maps to biolink:binds which is depreciated
    "kd": f"RO:0002436",
    "kd1": f"RO:0002436",  # RO:0002434 maps to biolink:related_to
    "ki": f"{DGIDB}:inhibitor",
    "km": f"RO:0002436",
    "ks": f"RO:0002436",
    "modulator": f"{DGIDB}:modulator",
    "mic": f"RO:0002434",  # What is this referring to?
    "mpc": f"RO:0002434",  # What is this referring to?
    "negative_modulator": f"{CHEMBL_MECHANISM}:negative_modulator",
    "negative_allosteric_modulator": f"{CHEMBL_MECHANISM}:negative_modulator",
    "opener": f"{CHEMBL_MECHANISM}:opener",
    "other": f"{DGIDB}:other",
    "partial_agonist": f"{DGIDB}:partial_agonist",
    "pa2": f"RO:0002434",  # What is this referring to?
    "pharmacological_chaperone": f"{DGIDB}:chaperone",
    "positive_allosteric_modulator": f"{CHEMBL_MECHANISM}:positive_modulator",
    "positive_modulator": f"{CHEMBL_MECHANISM}:positive_modulator",
    "releasing_agent": f"{CHEMBL_MECHANISM}:releasing_agent",
    "substrate": f"{CHEMBL_MECHANISM}:substrate",
    "xc50": f"RO:0002436"  # This is related to ec50 and ic50 both of which describe binding events
}
