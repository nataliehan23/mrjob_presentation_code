import hashlib

overall_ctr = 0.0091018504588536182

def obscure_id(id):
    return hashlib.md5(str(id)).hexdigest()
