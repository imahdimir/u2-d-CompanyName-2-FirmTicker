##

"""

    """

##

import pandas as pd

from githubdata import GithubData
from mirutil import funcs as mf


repo_url = 'https://github.com/imahdimir/raw-d-Listed-Firms-in-IFB'
btic_repo_url = 'https://github.com/imahdimir/d-uniq-BaseTickers'

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
  for fn in fns.keys() :
    _df = pd.read_excel(repo.local_path / f'{fn}.xlsx')
    df = pd.concat([df , _df])
  ##
  df = df.drop(columns = 'ردیف')
  ##
  df = df.dropna(subset = namaad)
  ##
  df[namaad] = df[namaad].apply(mf.norm_fa_str)
  ##
  df = df[[namaad , naam]]
  ##

  btic_repo = GithubData(btic_repo_url)
  btic_repo.clone_overwrite_last_version()
  ##
  bdfpn = btic_repo.data_fps[0]
  bdf = pd.read_parquet(bdfpn)
  bdf = bdf.reset_index()
  ##
  bdf = bdf.merge(df , left_on = btick , right_on = namaad , how = 'left')
  ##
  msk = bdf[cname].isna()
  bdf.loc[msk , cname] = bdf[naam]
  ##
  msk = bdf[cname].isna()
  df1 = bdf[msk]
  ##
  bdf = bdf[[btick , cname]]
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