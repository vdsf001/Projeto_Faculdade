import os
import requests
from io import BytesIO
from zipfile import ZipFile


def ensure_dir(dir_name):
    """TODO
    """
    try:
        os.makedirs(dir_name)
        return True
    except Exception:
        return False

def pad_month_num(month):
    """TODO
    """
    if month < 10:
        month = '0' + str(month)
    return str(month)


def make_ans_filename(year, month, region):
    """TODO
    """
    return 'ben' + str(year) + pad_month_num(month) + '_' + region + '.zip'


def make_ans_url(base_url, year, month, region):
    """TODO
    """
    url = [
        base_url,
        str(year) + pad_month_num(month),
        make_ans_filename(year, month, region)
    ]
    return '/'.join(url)


def download_ans_file(base_url, year, month, region, destination_folder):
    """TODO
    """

    ensure_dir(destination_folder)
    url = make_ans_url(base_url, year, month, region)
    print(url)
    content = requests.get(url)

    f = ZipFile(BytesIO(content.content))
    f.extractall(destination_folder)

    made_file = os.path.join(
        destination_folder,
        make_ans_filename(year, month, region)).replace('.zip', '.csv')
    return made_file


def upload_to_gcs(filepath, bucket_name, client, folders=None):
    """TODO
    """
    if isinstance(folders, list):
        folders = '/'.join(folders)
    elif not isinstance(folders, str) and folders is not None:
        raise TypeError('folders must be str, list or none')

    bucket = client.get_bucket(bucket_name)
    filename = 'ANS_' + filepath.split(os.sep)[-1]

    if folders:
        filename = '/'.join([folders, filename])

    blob = bucket.blob(filename)
    blob.upload_from_filename(filepath)
    return 'gs://{}/{}'.format(bucket_name, filename)


def find_correct_encoding(fpath):
    """Abre o arquivo com o melhor encoding encontrado. Começa com utf-8 e
    tenta cada outro encoding até que f.readlines() funcione ou retorna False

    Args:
        fpath(str, mandatory):
            o caminho do arquivo a ser lido

    Returns:
        str: o encoding "correto" para o arquivo
    """
    encodings = [
        'utf-8',
        'utf-8-sig',
        'utf-16',
        'cp1252',  # Windows 1252
        'iso-8859-1',
        'iso-8859-2',
        'iso-8859-3',
    ]

    for enc in encodings:
        num_lines = 0
        try:
            with open(fpath, 'r', encoding=enc) as f:
                line = f.readline()
                while line:
                    num_lines += 1
                    if num_lines >= 100:
                        break
                    line = f.readline()
                return enc
        except FileNotFoundError:
            raise RuntimeError('Arquivo nao encontrado: {}'.format(fpath))
        except PermissionError:
            raise RuntimeError(
                'Acesso negado ao arquivo: {}'.format(fpath))
        except Exception:
            continue
        return enc
    return False