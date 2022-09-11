"""

    """

import json
import pandas as pd

from githubdata import GithubData , get_data_from_github
from mirutil.df_utils import save_as_prq_wo_index as sprq


class GDUrl :
    with open('gdu.json' , 'r') as fi :
        gj = json.load(fi)

    cur = gj['cur']
    src0 = gj['src0']
    src1 = gj['src1']
    src2 = gj['src2']
    src3 = gj['src3']
    src4 = gj['src4']
    src5 = gj['src5']
    src6 = gj['src6']
    trg = gj['trg']

gu = GDUrl()

class ColName :
    radif = 'ردیف'
    namaad = 'نماد'
    naam = 'نام شرکت'
    btick = 'BaseTicker'
    cname = 'CompanyName'
    ftic = 'FirmTicker'

c = ColName()

def main() :
    pass

    ##
    df = pd.DataFrame()

    for gdu in [gu.src0 , gu.src1 , gu.src2 , gu.src3 , gu.src4 , gu.src5] :
        _df = get_data_from_github(gdu)

        df = pd.concat([df , _df] , axis = 0 , ignore_index = True)

    ##

    df = df[[c.naam , c.namaad]]
    ##
    df = df.dropna()
    ##

    gds = GithubData(gu.src6)
    gds.overwriting_clone()
    ##
    ds = gds.read_data()

    ##

    da = ds.merge(df , left_on = c.ftic , right_on = c.namaad , how = 'left')
    ##

    da = da[[c.ftic , c.naam]]
    ##
    da = da.dropna()
    ##
    da = da.rename(
            columns = {
                    c.naam : c.cname
                    }
            )
    ##

    gdt = GithubData(gu.trg)
    gdt.overwriting_clone()
    ##
    dft = gdt.read_data()
    ##

    dft = pd.concat([dft , da] , axis = 0 , ignore_index = True)

    ##
    dft = dft.drop_duplicates()
    ##

    assert dft[c.cname].is_unique

    ##
    sprq(dft , gdt.data_fp)

    ##
    msg = 'added 2 data by: '
    msg += gu.cur

    ##

    gdt.commit_and_push(msg)

    ##

    gdt.rmdir()
    gds.rmdir()


    ##

##
if __name__ == '__main__' :
    main()

##
