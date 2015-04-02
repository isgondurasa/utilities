#coding: utf-8
import os
import glob
import shutil
from collections import defaultdict
from optparse import OptionParser


def search_for_additional_files(filepath, filename):
    u"""
        return files with matches names
    """
    return filter(lambda f: f.split(".")[1] != 'pdf', glob.glob(os.path.join(filepath, filename+"*.*")))


def run():
    u"""
        moving pdf and same namedfiles to pdf named dir
    """
    parser = OptionParser()
    parser.add_option("-p", "--path", dest="path", default="")

    options, _ = parser.parse_args()
    filepath = options.path

    files = glob.glob(os.path.join(filepath, '*.pdf'))
    print "FOUND {} pdf files".format(len(files))
    result = defaultdict(list)

    for pdf_file in files:
        pdf_filename = pdf_file.split(".")[0]
        result[pdf_filename] = search_for_additional_files(filepath, pdf_filename)

    for pdf_filename, add_files in result.iteritems():

        try:
            os.mkdir(os.path.join(filepath, pdf_filename))
        except OSError as e:
            print e

        source_pdf = os.path.join(filepath, pdf_filename + ".pdf")
        dest_pdf = os.path.join(filepath, pdf_filename, pdf_filename + ".pdf")
        shutil.move(source_pdf, dest_pdf)
        for add_file in add_files:
            filepath_src = os.path.join(filepath, add_file)
            filepath_dest = os.path.join(filepath, pdf_filename, add_file)
            print "MOVING {} to {}".format(add_file, filepath_dest)
            shutil.move(filepath_src, filepath_dest)

if __name__ == "__main__":
    run()
