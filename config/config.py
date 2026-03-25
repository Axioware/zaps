import os

# API Keys & Secrets
ADMIN_SECRET_KEY = "12345678" 
SF_REFRESH_TOKEN = "YOUR_SF_REFRESH_TOKEN"
SF_INSTANCE_URL = "https://leftmain-4606.lightning.force.com/"

ELEVEN_LABS_KEY = "sk_2c83ea518c2865448915d6d42e7c1149a7000de253743c7f"
ELEVEN_AGENT_ID = "agent_6901kh8qcn0ye0na34bt7t5t72ee"

# Mapping logic
AREA_CODE_MAP = {
    '213': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '310': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '323': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '357': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '424': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '562': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '626': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '661': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '738': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '747': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '805': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '818': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '820': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '209': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '279': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '341': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '350': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '408': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '415': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '510': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '530': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '559': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '628': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '650': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '669': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '707': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '831': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '837': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '916': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '925': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '619': 'phnum_1101kjcdrvnjejwr4j3g008g264r',
    '858': 'phnum_1101kjcdrvnjejwr4j3g008g264r',
    '369': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '442': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '760': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '840': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '909': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '959': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '657': 'phnum_0001khx5hqh7fbkryh4ymgsnwns5',
    '714': 'phnum_0001khx5hqh7fbkryh4ymgsnwns5',
    '949': 'phnum_0001khx5hqh7fbkryh4ymgsnwns5',
    
}


DEFAULT_PHONE = 'phnum_7801k60w0n6vecav04d878ej4g7x'