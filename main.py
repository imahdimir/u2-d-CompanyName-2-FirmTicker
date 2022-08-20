##

"""

    """

##

from pathlib import Path

import pandas as pd

from githubdata import GithubData
from mirutil import funcs as mf


repo_url = 'https://github.com/imahdimir/raw-d-Listed-Firms-in-IFB'
btic_repo_url = 'https://github.com/imahdimir/d-Unique-BaseTickers-TSETMC'

namaad = 'نماد'
naam = 'نام شرکت'
btick = 'BaseTicker'
cname = 'CompanyName'

def main() :

  pass

  ##
  repo = GithubData(repo_url)
  repo.clone_overwrite_last_version()
  ##
  fns = {
      "IFB-Bazar-e-Paye-Zard"    : None ,
      "IFB-Bazar-e-Avval"        : None ,
      "IFB-Bazar-e-SME"          : None ,
      "IFB-Bazar-e-Paye-Ghermez" : None ,
      "IFB-Bazar-e-Paye-Narenji" : None ,
      "IFB-Bazar-e-Dovvom"       : None ,
      }

  ##
  df = pd.DataFrame()
  for fn in fns.keys():
    _df = pd.read_excel(repo.local_path / f'{fn}.xlsx')
    df = pd.concat([df, _df])
  ##
  df = df.drop(columns = 'ردیف')
  ##
  df = df.dropna(subset = namaad)
  ##
  df[namaad] = df[namaad].apply(mf.norm_fa_str)
  ##
  df = df[[namaad, naam]]
  ##

  btic_repo = GithubData(btic_repo_url)
  btic_repo.clone_overwrite_last_version()
  ##
  bdfpn = btic_repo.data_fps[0]
  bdf = pd.read_parquet(bdfpn)
  bdf = bdf.reset_index()
  ##
  bdf = bdf.merge(df, left_on = btick, right_on = namaad, how = 'left')
  ##
  msk = bdf[cname].isna()
  bdf.loc[msk, cname] = bdf[naam]
  ##
  msk = bdf[cname].isna()
  df1 = bdf[msk]
  ##
  bdf = bdf[[btick, cname]]
  bdf = bdf.set_index(btick)
  ##
  bdf.to_parquet(bdfpn)
  ##
  commit_msg = 'Filled null CompanyName from IFB raw data in repo: https://github.com/imahdimir/raw-d-Listed-Firms-in-IFB'
  btic_repo.commit_and_push_to_github_data_target(commit_msg)

  ##
  repo.rmdir()
  btic_repo.rmdir()


  ##

  ##

  ##

  ##
  dfpn = repo.local_path / 'IFB-IPOs.xlsx'
  df = pd.read_excel(dfpn)
  ##
  df['bt'] = df['نام نماد'].str[:-1]
  ##
  ptr = '\D+'
  df['n'] = df['bt'].str.fullmatch(ptr)
  ##
  msk = df['n']
  df.loc[msk, btick] = df['bt']
  ##
  df.loc[~ msk, btick] = df['bt'].str[:-1]
  ##
  df.columns
  ##
  df['samedate'] = df['تاریخ ابتدای بازه عرضه'].eq(df['تاریخ انتهای بازه عرضه'])
  ##
  df[ipojd] = df['تاریخ انتهای بازه عرضه']
  ##
  df[ipojd] = df[ipojd].str.replace('/', '-')
  ##

  ipo_repo = GithubData(ipo_repo_url)
  ipo_repo.clone_overwrite_last_version()
  ##
  ipo_fpn = ipo_repo.data_fps[0]
  idf = pd.read_parquet(ipo_fpn)
  ##
  idf = idf.reset_index()

  ##
  df = df[[btick, ipojd]]
  idf = idf.merge(df, how = 'outer')
  ##
  idf.to_parquet(ipo_fpn)
  ##
  commit_msg = 'added IPO dates from github repo: imahdimir/fr-raw-d-IPOs-4-IFB-add-2-d-BaseTicker-IPOJDate-1'
  ipo_repo.commit_and_push_to_github_data_target(commit_msg)

  ##
  ipo_repo.rmdir()
  repo.rmdir()

  ##





  ##




  ##


##