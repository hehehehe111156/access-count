# -*- coding: utf8 -*-
# SCF configures the COS trigger, obtains the file uploading information from COS, and downloads it to the local temporary disk 'tmp' of SCF.
# SCF配置COS触发，从COS获取文件上传信息，并下载到SCF的本地临时磁盘tmp
from qcloud_cos_v5 import CosConfig
from qcloud_cos_v5 import CosS3Client
from qcloud_cos_v5 import CosServiceError
from qcloud_cos_v5 import CosClientError
import sys
import logging
import os
import json
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO) # 默认打印 INFO 级别日志，可根据需要调整为 DEBUG、WARNING、ERROR、CRITICAL 级日志

appid = 1312797073  # Please replace with your APPID. 请替换为您的 APPID
secret_id = u'AKIDhEWXHm0xd2A1e06vTkkTv60dz4yd2A1c'  # Please replace with your SecretId. 请替换为您的 SecretId
secret_key = u'MSHp78rJVVFivsYx5wWprc6IDsFMCl34'  # Please replace with your SecretKey. 请替换为您的 SecretKey
region = u'ap-beijing'  # Please replace with the region where COS bucket located. 请替换为您bucket 所在的地域
token = ''

config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region=region, Token=token)
client = CosS3Client(config)
logger = logging.getLogger()

# 用户的 bucket 信息
test_bucket = 'usefortest-1312797073'
start_prefix = 'cos-access-log/'
# 对象存储依赖 分隔符 '/' 来模拟目录语义，
# 使用默认的空分隔符可以列出目录下面的所有子节点，实现类似本地目录递归的效果,
# 如果 delimiter 设置为 "/"，则需要在程序里递归处理子目录
delimiter = ''

# 需要检索的文件名称
files_count = 'index.md'



def main_handler(event, context):
    logger.info("start main handler")
    for record in event['Records']:
        try:
            bucket = record['cos']['cosBucket']['name'] + '-' + str(appid)
            key = record['cos']['cosObject']['key']
            key = key.replace('/' + str(appid) + '/' + record['cos']['cosBucket']['name'] + '/', '', 1)
            logger.info("Key is " + key)

            # download object from cos
            logger.info("Get from [%s] to download file [%s]" % (bucket, key))
            download_path = '/tmp/{}'.format(key)
            try:
                response = client.get_object(Bucket=bucket, Key=key, )
                response['Body'].get_stream_to_file(download_path)
            except CosServiceError as e:
                print(e.get_error_code())
                print(e.get_error_msg())
                print(e.get_resource_location())
                return "Fail"
            logger.info("Download file [%s] Success" % key)

        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. '.format(key, bucket))
            raise e
            return "Fail"

    return "Success"
