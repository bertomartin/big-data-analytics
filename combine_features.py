from glob import glob
import os
import pdb

def combine_features():
    '''()->None
    take all the *_feature files in the ./stocks directory and append them to each other.
    '''
    combined_feature_file = "./combined_features.csv"
    mode = 'a' if os.path.exists(combined_feature_file) else 'w'
    fout = open(combined_feature_file, mode)
    feature_files = glob('./stocks/*_features.csv')

    for f in feature_files:
        print f
        fobj = open(f, 'rt')
        lines = fobj.readlines()
        print "lines: ", len(lines)
        print "\n"
        fobj.close()
        fout.writelines(lines[1:])
    fout.close()

if __name__ == '__main__':
    combine_features()
