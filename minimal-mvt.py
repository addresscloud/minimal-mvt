import json
import boto3
import base64
import os

# Fetch tiles from an Aurora Postgres DB using the AWS Data API
class TileRequestHandler:

    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.rdsData = boto3.client('rds-data')


    # Search REQUEST_PATH for /{z}/{x}/{y}.{format} patterns
    def pathToTile(self, params):

        if (params):
            return { 
                'zoom': int(params['z']),
                'x': int(params['x']),
                'y': int(params['y']),
                'format': 'mvt' # hard code format for demo.
            }
        else:
            return None

    # Do we have all keys we need? 
    # Do the tile x/y coordinates make sense at this zoom level?
    def tileIsValid(self, tile):
        if not ('x' in tile and 'y' in tile and 'zoom' in tile):
            return False
        if 'format' not in tile or tile['format'] not in ['pbf', 'mvt']:
            return False
        size = 2 ** tile['zoom'];
        if tile['x'] >= size or tile['y'] >= size:
            return False
        if tile['x'] < 0 or tile['y'] < 0:
            return False
        return True


    # Calculate envelope in "Spherical Mercator" (https://epsg.io/3857)
    def tileToEnvelope(self, tile):
        # Width of world in EPSG:3857
        worldMercMax = 20037508.3427892
        worldMercMin = -1 * worldMercMax
        worldMercSize = worldMercMax - worldMercMin
        # Width in tiles
        worldTileSize = 2 ** tile['zoom']
        # Tile width in EPSG:3857
        tileMercSize = worldMercSize / worldTileSize
        # Calculate geographic bounds from tile coordinates
        # XYZ tile coordinates are in "image space" so origin is
        # top-left, not bottom right
        env = dict()
        env['xmin'] = worldMercMin + tileMercSize * tile['x']
        env['xmax'] = worldMercMin + tileMercSize * (tile['x'] + 1)
        env['ymin'] = worldMercMax - tileMercSize * (tile['y'] + 1)
        env['ymax'] = worldMercMax - tileMercSize * (tile['y'])
        return env


    # Generate SQL to materialize a query envelope in EPSG:3857.
    # Densify the edges a little so the envelope can be
    # safely converted to other coordinate systems.
    def envelopeToBoundsSQL(self, env):
        DENSIFY_FACTOR = 4
        env['segSize'] = (env['xmax'] - env['xmin'])/DENSIFY_FACTOR
        sql_tmpl = 'ST_Segmentize(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 3857),{segSize})'
        return sql_tmpl.format(**env)


    # Generate a SQL query to pull a tile worth of MVT data
    # from the table of interest.        
    def envelopeToSQL(self, env):
        tbl = TABLE.copy()
        tbl['env'] = self.envelopeToBoundsSQL(env)
        # Materialize the bounds
        # Select the relevant geometry and clip to MVT bounds
        # Convert to MVT format
        sql_tmpl = """
            WITH 
            bounds AS (
                SELECT {env} AS geom, 
                       {env}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT ST_AsMVTGeom(ST_Transform(ST_Force2D(t.{geomColumn}), 3857), bounds.b2d) AS geom, 
                       {attrColumns}
                FROM {table} t, bounds
                WHERE ST_Intersects(t.{geomColumn}, ST_Transform(bounds.geom, {srid}))
            ) 
            SELECT ST_AsMVT(mvtgeom.*) FROM mvtgeom
        """
        return sql_tmpl.format(**tbl)

    # Run tile query SQL and return error on failure conditions
    def sqlToPbf(self, sql):
        pbf = self.rdsData.execute_statement(
            resourceArn = self.database['cluster_arn'], 
            secretArn = self.database['secret_arn'], 
            database = self.database['database'], 
            sql = sql)

        # TODO - error handling (return None)
        return pbf['records'][0][0]['blobValue']

# Database to connect to
DATABASE = {
    'cluster_arn': os.environ['cluster_arn'],
    'secret_arn': os.environ['secret_arn'],
    'database': os.environ['database']
    }

# Table to query for MVT data, and columns to
# include in the tiles.
TABLE = {
    'table':       os.environ['table'],
    'srid':        os.environ['srid'],
    'geomColumn':  'geom',
    'attrColumns': 'id'
    } 

mvt = TileRequestHandler(DATABASE, TABLE)

# Respond to API GW requests
def lambda_handler(event, context):
    # http://localhost:8080/9/255/170.mvt
    tile = mvt.pathToTile(event['pathParameters'])
    
    if not (tile and mvt.tileIsValid(tile)):
        return {
            'statusCode': 400,
            'headers': {},
            'body': json.dumps("invalid tile path: %s" % (self.path))
        }

    env = mvt.tileToEnvelope(tile)
    sql = mvt.envelopeToSQL(env)
    pbf = mvt.sqlToPbf(sql)

    print("path: %s\ntile: %s\n env: %s" % (event['pathParameters'], tile, env))
    print("sql: %s" % (sql))
    
    return {
        'statusCode': 200,
        'isBase64Encoded': True,
        'headers': {"Content-type": "application/vnd.mapbox-vector-tile",
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,GET'
        },
        'body': base64.b64encode(pbf).decode("utf-8")
    }
    
