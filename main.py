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

btick = 'BaseTicker'

def main() :

  pass

  ##
  repo = GithubData(repo_url)
  repo.clone_overwrite_last_version()
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