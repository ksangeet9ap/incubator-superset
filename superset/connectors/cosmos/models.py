from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, timedelta
import json
import logging
from multiprocessing.pool import ThreadPool
import re

from dateutil.parser import parse as dparse
from flask import escape, Markup
from flask_appbuilder import Model
from flask_appbuilder.models.decorators import renders
from flask_babel import lazy_gettext as _

import requests
from six import string_types
import sqlalchemy as sa
from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.orm import backref, relationship

from superset import conf, db, import_util, security_manager, utils
from superset.connectors.base.models import BaseColumn, BaseDatasource, BaseMetric
from superset.exceptions import MetricPermException, SupersetException
from superset.models.helpers import (
    AuditMixinNullable, ImportMixin, QueryResult, set_perm,
)
from superset.utils import (
    DimSelector, DTTM_ALIAS, flasher,
)

from pymongo import MongoClient
import datetime
import pandas as pd
import pprint

class CosmosCluster(Model, AuditMixinNullable, ImportMixin):
    __tablename__ = 'cosmos_clusters'
    type = 'cosmos'

    id = Column(Integer, primary_key=True)
    verbose_name = Column(String(250), unique=True)
    cluster_name = Column(String(250), unique=True)
    mongo_url = Column(String(250))
    metadata_last_refreshed = Column(DateTime)
    cache_timeout = Column(Integer)

    export_fields = ('cluster_name', 'mongo_url', 'cache_timeout')
    export_children = ['datasources']

    def __repr__(self):
        return self.verbose_name if self.verbose_name else self.cluster_name

    def __html__(self):
        return self.__repr__()

    @property
    def data(self):
        return {
            'name': self.cluster_name,
            'backend': 'cosmos',
        }

    def get_cosmos_client(self):
        return MongoClient(self.mongo_url)

    def get_datasources(self):
        client = self.get_cosmos_client()
        return client[self.cluster_name].collection_names()

    def refresh_datasources(
            self,
            datasource_name=None,
            merge_flag=None,
            refreshAll=None):
        ds_list = self.get_datasources()
        blacklist = conf.get('COSMOS_DATA_SOURCE_BLACKLIST', [])
        ds_refresh = []
        if not datasource_name:
            ds_refresh = ds_list
        elif datasource_name not in blacklist and datasource_name in ds_list:
            ds_refresh.append(datasource_name)
        else:
            return
        self.refresh(ds_refresh, merge_flag, refreshAll)

    def refresh(self, datasource_names, merge_flag, refreshAll):
        def get_type(x): # Currently limited, add accordingly
            t = 'UNKNOWN'
            if isinstance(x, basestring):
                t = 'STRING'
            elif isinstance(x, int):
                t = 'INT'
            elif isinstance(x, long):
                t = 'LONG'
            elif isinstance(x, float):
                t = 'DOUBLE'
            elif isinstance(x, datetime.datetime):
                t = 'DATETIME'
            return t
        session = db.session
        ds_list = (
            session.query(CosmosDatasource)
            .filter(CosmosDatasource.cluster_name == self.cluster_name)
            .filter(CosmosDatasource.datasource_name.in_(datasource_names))
        )
        ds_map = {ds.name: ds for ds in ds_list}
        client = self.get_cosmos_client()
        for ds_name in datasource_names:
            datasource = ds_map.get(ds_name, None)
            if not datasource:
                datasource = CosmosDatasource(datasource_name=ds_name)
                with session.no_autoflush:
                    session.add(datasource)
                flasher('Adding new datasource [{}]'.format(ds_name))
                ds_map[ds_name] = datasource
            elif refreshAll:
                flasher('Refreshing datasource [{}]'.format(ds_name))
            else:
                del ds_map[ds_name]
                continue
            datasource.cluster = self
            datasource.merge_flag = merge_flag
        session.flush()

        # Bug - Safely assuming all the records follow the same schema
        # Assuming no nested json is present
        ds_refresh = list(ds_map.values())
        dttm_cols = []
        for i in range(len(ds_refresh)):
            datasource = ds_refresh[i]
            # Currently checking data types by extracting one row and checking the types of each col
            sample = client[self.cluster_name][datasource.name].find_one()
            cols = sample.keys()
            if not cols: continue
            col_objs_list = (
                session.query(CosmosColumn)
                .filter(CosmosColumn.datasource_id == datasource.id)
                .filter(CosmosColumn.column_name.in_(cols))
            )
            col_objs = {col.column_name: col for col in col_objs_list}
            for col in cols:
                col_obj = col_objs.get(col)
                if not col_obj:
                    col_obj = CosmosColumn(
                        datasource_id=datasource.id,
                        column_name=col
                    )
                    with session.no_autoflush:
                        session.add(col_obj)
                col_obj.type = get_type(sample[col])
                col_obj.datasource = datasource
                if col_obj.type == 'STRING':
                    col_obj.groupby = True
                    col_obj.filterable = True
                    # col_obj.count_distinct = True # Will figure out later
                if col_obj.is_num:
                    col_obj.groupby = True
                    col_obj.filterable = True
                    col_obj.sum = True
                    col_obj.min = True
                    col_obj.max = True
                    col_obj.avg = True
                # Have to think into the date part to implement granualarity and time series charts
                if col_obj.is_time:
                    if not datasource.main_dttm_col:
                        datasource.main_dttm_col = col_obj.column_name
                    col_obj.is_dttm = True
                #     dttm_cols.append(col_obj.column_name)
            # datasource.dttm_cols = dttm_cols
            datasource.refresh_metrics()
        session.commit()

    @property
    def perm(self):
        return '[{obj.cluster_name}].(id:{obj.id})'.format(obj=self)

    def get_perm(self):
        return self.perm

    @property
    def name(self):
        return self.verbose_name if self.verbose_name else self.cluster_name

    @property
    def unique_name(self):
        return self.verbose_name if self.verbose_name else self.cluster_name

class CosmosColumn(Model, BaseColumn):
    __tablename__ = 'cosmos_columns'

    datasource_id = Column(
        Integer,
        ForeignKey('cosmos_datasources.id')
    )
    datasource = relationship(
        'CosmosDatasource',
        backref=backref('columns', cascade='all, delete-orphan'),
        enable_typechecks=False
    )
    is_dttm = Column(Boolean, default=False)
    json = Column(Text)

    export_fields = (
        'datasource_name', 'column_name', 'is_active', 'type', 'groupby',
        'sum', 'avg', 'max', 'min', 'filterable',
        'description', 'is_dttm'
    )

    @property
    def expression(self):
        return self.json

    def get_metrics(self):
        metrics = {}
        metrics['count'] = CosmosMetric(
            metric_name='count',
            verbose_name='count',
            metric_type='count',
            json=json.dumps({"count": {"$sum": 1}})
        )

        if self.type in ('DOUBLE', 'FLOAT'):
            corrected_type = 'DOUBLE'
        else:
            corrected_type = self.type

        if self.sum and self.is_num:
            mt = corrected_type.lower() + 'Sum'
            name = 'sum__' + self.column_name
            metrics[name] = CosmosMetric(
                metric_name=name,
                metric_type='sum',
                verbose_name=name,
                json=json.dumps({name: {"$sum": "${}".format(self.column_name)}})
            )

        if self.avg and self.is_num:
            mt = corrected_type.lower() + 'Avg'
            name = 'avg__' + self.column_name
            metrics[name] = CosmosMetric(
                metric_name=name,
                metric_type='avg',
                verbose_name=name,
                json=json.dumps({name: {'$avg': "${}".format(self.column_name)}})
            )

        if self.min and self.is_num:
            mt = corrected_type.lower() + 'Min'
            name = 'min__' + self.column_name
            metrics[name] = CosmosMetric(
                metric_name=name,
                metric_type='min',
                verbose_name=name,
                json=json.dumps({name: {'$min': '${}'.format(self.column_name)}})
            )

        if self.max and self.is_num:
            mt = corrected_type.lower() + 'Max'
            name = 'max__' + self.column_name
            metrics[name] = CosmosMetric(
                metric_name=name,
                metric_type='max',
                verbose_name=name,
                json=json.dumps({name: {'$max': '${}'.format(self.column_name)}})
            )

        return metrics

    def refresh_metrics(self):
        metrics = self.get_metrics()
        dbmetrics = (
            db.session.query(CosmosMetric)
            .filter(CosmosMetric.datasource_id == self.datasource_id)
            .filter(CosmosMetric.metric_name.in_(metrics.keys()))
        )
        dbmetrics = {metric.metric_name: metric for metric in dbmetrics}
        for metric in metrics.values():
            dbmetric = dbmetrics.get(metric.metric_name)
            if dbmetric:
                for attr in ['json', 'metric_type']:
                    setattr(dbmetric, attr, getattr(metric, attr))
            else:
                with db.session.no_autoflush:
                    metric.datasource_id = self.datasource_id
                    db.session.add(metric)

    @classmethod
    def import_obj(cls, i_column):
        def lookup_obj(lookup_column):
            return db.session.query(CosmosColumn).filter(
                CosmosColumn.datasource_id == lookup_column.datasource_id,
                CosmosColumn.column_name == lookup_column.column_name
            ).first()
        return import_util.import_simple_obj(db.session, i_column, lookup_obj)

class CosmosMetric(Model, BaseMetric):
    __tablename__ = "cosmos_metrics"
    __table_args__ = (UniqueConstraint('metric_name', 'datasource_id'),)
    datasource_id = Column(
        Integer,
        ForeignKey('cosmos_datasources.id')
    )
    datasource = relationship(
        'CosmosDatasource',
        backref=backref('metrics', cascade='all, delete-orphan'),
        enable_typechecks=False
    )
    json = Column(Text)

    export_fields = (
        'metric_name', 'verbose_name', 'metric_type', 'datasource_id',
        'json', 'description', 'is_restricted', 'd3format',
    )
    export_parent = 'datasource'

    @property
    def expression(self):
        return self.json

    @property
    def json_obj(self):
        try:
            obj = json.loads(self.json)
        except Exception:
            obj = {}
        return obj

    @property
    def perm(self):
        return (
            '{parent_name}.[{obj.metric_name}](id:{obj.id})'
        ).format(obj=self,
                 parent_name=self.datasource.full_name,
                 ) if self.datasource else None

    @classmethod
    def import_obj(cls, i_metric):
        def lookup_obj(lookup_metric):
            return db.session.query(CosmosMetric).filter(
                CosmosMetric.datasource_id == lookup_metric.datasource_id,
                CosmosMetric.metric_name == lookup_metric.metric_name).first()
        return import_util.import_simple_obj(db.session, i_metric, lookup_obj)

class CosmosDatasource(Model, BaseDatasource):
    __tablename__ = 'cosmos_datasources'
    __table_args__ = (UniqueConstraint('datasource_name', 'cluster_name'),)

    type = 'cosmos'
    query_language = 'json'
    cluster_class = CosmosCluster
    metric_class = CosmosMetric
    column_class = CosmosColumn

    baselink = 'cosmosdatasourcemodelview'

    datasource_name = Column(String(255))
    main_dttm_col = Column(String(250))
    is_hidden = Column(Boolean, default=False)
    filter_select_enabled = Column(Boolean, default=True)
    fetch_values_from = Column(String(100))
    cluster_name = Column(String(250), ForeignKey('cosmos_clusters.cluster_name'))
    cluster = relationship(
        'CosmosCluster', backref='datasources', foreign_keys=[cluster_name]
    )
    user_id = Column(Integer, ForeignKey('ab_user.id'))
    owner = relationship(
        security_manager.user_model,
        backref=backref('cosmos_datasources', cascade='all, delete-orphan'),
        foreign_keys=[user_id]
    )
    UniqueConstraint('cluster_name', 'datasource_name')

    export_fields = (
        'datasource_name', 'is_hidden', 'description', 'default_endpoint',
        'cluster_name', 'offset', 'cache_timeout', 'params',
    )

    export_parent = 'cluster'
    export_children = ['columns', 'metrics']

    @property
    def database(self):
        return self.cluster

    @property
    def connection(self):
        return str(self.database)

    # @property
    # def main_dttm_col(self):
    #     return None
    #
    # @main_dttm_col.setter
    # def main_dttm_col(self, date):
    #     self.main_dttm_col = date

    @property
    def dttm_cols(self):
        l = [c.column_name for c in self.columns if c.type is 'DATETIME']
        if self.main_dttm_col and self.main_dttm_col not in l:
            l.append(self.main_dttm_col)
        return l

    @property
    def any_dttm_col(self):
        cols = self.dttm_cols
        if cols:
            return cols[0]

    #
    # @dttm_cols.setter
    # def dttm_cols(self, date_list):
    #     self.dttm_cols = date_list

    @property
    def num_cols(self):
        return [c.column_name for c in self.columns if c.is_num]

    @property
    def name(self):
        return self.datasource_name

    @property
    def schema(self):
        ds_name = self.datasource_name or ''
        name_pieces = ds_name.split('.')
        if len(name_pieces) > 1:
            return name_pieces[0]
        else:
            return None

    @property
    def schema_perm(self):
        """Returns schema permission if present, cluster one otherwise."""
        return security_manager.get_schema_perm(self.cluster, self.schema)

    def get_perm(self):
        return (
            '[{obj.cluster_name}].[{obj.datasource_name}]'
            '(id:{obj.id})').format(obj=self)

    @property
    def link(self):
        name = escape(self.datasource_name)
        return Markup('<a href="{self.url}">{name}</a>').format(**locals())

    @property
    def full_name(self):
        return utils.get_datasource_full_name(
            self.cluster_name, self.datasource_name)

    @property
    def time_column_grains(self):
        return {
            # 'time_columns': [
            #     'all', '5 seconds', '30 seconds', '1 minute',
            #     '5 minutes', '1 hour', '6 hour', '1 day', '7 days',
            #     'week', 'week_starting_sunday', 'week_ending_saturday',
            #     'month',
            # ],
            'time_columns': self.dttm_cols,
            'time_grains': ['now'],
        }

    def __repr__(self):
        return self.datasource_name

    @renders('datasource_name')
    def datasource_link(self):
        url = '/superset/explore/{obj.type}/{obj.id}/'.format(obj=self)
        name = escape(self.datasource_name)
        return Markup('<a href="{url}">{name}</a>'.format(**locals()))

    def get_metric_obj(self, metric_name):
        return [
            m.json_obj for m in self.metrics
            if m.metric_name == metric_name
        ][0]

    @classmethod
    def import_obj(cls, i_datasource, import_time=None):
        """Imports the datasource from the object to the database.

         Metrics and columns and datasource will be overridden if exists.
         This function can be used to import/export dashboards between multiple
         superset instances. Audit metadata isn't copies over.
        """
        def lookup_datasource(d):
            return db.session.query(CosmosDatasource).filter(
                CosmosDatasource.datasource_name == d.datasource_name,
                CosmosCluster.cluster_name == d.cluster_name,
            ).first()

        def lookup_cluster(d):
            return db.session.query(CosmosCluster).filter_by(
                cluster_name=d.cluster_name).one()
        return import_util.import_datasource(
            db.session, i_datasource, lookup_cluster, lookup_datasource,
            import_time)

    def refresh_metrics(self):
        for col in self.columns:
            col.refresh_metrics()

    @property
    def data(self):
        d = super(CosmosDatasource, self).data
        if self.type == 'cosmos':
            # grains = self.database.grains() or []
            # if grains:
            #     grains = [(g.duration, g.name) for g in grains]
            d['granularity_sqla'] = utils.choicify(self.dttm_cols)
            d['time_grain_sqla'] = ['week']
        return d

    def values_for_column(self, column_name, limit=10000):
        """Retrieve some values for the given column"""
        logging.info('Getting values for column [{}] limited to [{}]'.format(column_name, limit))
        client = self.cluster.get_cosmos_client()
        db = client[self.cluster_name]
        data = db[self.datasource_name]\
            .find({}, {column_name: 1, '_id': 0})\
            .sort([(column_name, 1)])\
            .limit(limit).distinct(column_name)
        df = pd.DataFrame(list(data))
        return [row[0] for row in df.to_records(index=False)]

    def get_query_str(self, query_obj, phase=1, client=None):
        return self.run_query(client=client, phase=phase, **query_obj)

    def run_query(  # noqa / cosmos
            self,
            groupby, metrics,
            granularity,
            from_dttm, to_dttm,
            filter=None,  # noqa
            is_timeseries=False,
            timeseries_limit=None,
            timeseries_limit_metric=None,
            row_limit=None,
            inner_from_dttm=None, inner_to_dttm=None,
            orderby=None,
            extras=None,  # noqa
            columns=None, phase=2, client=None,
            order_desc=True,
            prequeries=None,
            is_prequery=False,
    ):
        client = client or self.cluster.get_cosmos_client()
        row_limit = row_limit or conf.get('ROW_LIMIT')

        FILTER_MAP = {
            '>=': '$gte',
            '>': '$gt',
            '<=': '$lte',
            '<': '$lt',
            '==': '$eq',
            '!=': '$ne'
        }

        orderby = orderby or []
        projection_columns = []

        # if is_timeseries:
        #     raise Exception(_('Timeseries slices not supported.'))

        if not granularity and is_timeseries:
            raise Exception(_(
                'Datetime column not provided as part table configuration '
                'and is required by this type of chart'))

        if not groupby and not metrics and not columns:
            raise Exception(_('Empty query?'))

        if granularity not in self.dttm_cols:
            granularity = self.main_dttm_col

        pipeline = []
        metrics_dict = {m.metric_name: m for m in self.metrics}
        columns_dict = {c.column_name: c for c in self.columns}
        having = extras.get('having_druid')

        metrics_exprs = {'$group': {'_id': {}}}
        if groupby:
            for s in groupby:
                metrics_exprs['$group']['_id'][s] = '${}'.format(s)
            if is_timeseries:
                metrics_exprs['$group']['_id'][DTTM_ALIAS] = '${}'.format(granularity)

        if granularity:
            # dttm_col = columns_dict[granularity]
            time_grain = extras.get('time_grain_sqla')

            if time_grain:
                raise Exception('Time grain not yet supported')

            pipeline.append({"$match": {granularity: {"$gte": from_dttm, "$lte": to_dttm}}})

        if filter:
            pass

        # Have to work on the count logic
        main_metric_expr = None
        if not metrics_exprs['$group']['_id']:
            metrics_exprs = {'$group': {'_id': ''}}
            if is_timeseries:
                metrics_exprs = {'$group': {'_id': {DTTM_ALIAS: '${}'.format(granularity)}}}

        # Count logic is done. Need to work on count_distinct
        for m in metrics:
            if utils.is_adhoc_metric(m):
                expressionType = m.get('expressionType')
                if expressionType == utils.ADHOC_METRIC_EXPRESSION_TYPES['SIMPLE']:
                    column_name = m.get('column').get('column_name')
                    aggregate_column_name = m.get('aggregate').lower() + '__{}'.format(column_name)
                    # aggregate_column_name = m.get('aggregate').upper() + '({})'.format(column_name)
                    if aggregate_column_name == 'count__({})'.format(column_name):
                        metrics_exprs['$group'][aggregate_column_name] = {'$sum': 1}
                    elif aggregate_column_name == 'count_distinct__({})'.format(column_name):
                        raise Exception(_('Count distinct not yet supported.'))
                    else:
                        # pipeline.append({'${}'.format(m.get('aggregate').lower()): '${}'.format(column_name)})
                        metrics_exprs['$group'][aggregate_column_name] = {'${}'.format(m.get('aggregate').lower()): '${}'.format(column_name)}
                    if main_metric_expr is None:
                        main_metric_expr = aggregate_column_name
                elif expressionType == utils.ADHOC_METRIC_EXPRESSION_TYPES['SQL']:
                    raise Exception(_('Dont know what to do.')) # Dont know what to do as of now
            elif m in metrics_dict:
                # if m == 'count':
                #     metrics_exprs['$group'][m] = {'$sum': 1}
                #     main_metric_expr = m
                # else:
                #     pass # Dont know about this as of now
                metric_json_dict = json.loads(metrics_dict[m].json)
                metrics_exprs['$group'].update(metric_json_dict)
                if main_metric_expr is None:
                    main_metric_expr = metric_json_dict.keys()[0]
            else:
                raise Exception(_("Metric '{}' is not valid".format(m)))

        if not columns:
            pipeline.append(metrics_exprs)

        # if not metrics_exprs['$group']['_id']:
        #     pipeline.append({'$project': {'_id': 0}})
        # else:
        if not columns:
            id_exprs = {'$project': {}}
            for c in metrics_exprs['$group']:
                if c == '_id' and metrics_exprs['$group']['_id']:
                    for s in metrics_exprs['$group']['_id']:
                        id_exprs['$project'][s] = '$_id.{}'.format(s)
                        projection_columns.append(s)
                else:
                    id_exprs['$project'][c] = 1
                    if c != '_id':
                        projection_columns.append(c)
            id_exprs['$project']['_id'] = 0
            pipeline.append(id_exprs)

            if having:
                having_filter = {'$match': {'$and': []}}
                for agg_filter in having:
                    col = agg_filter.get('col')
                    op = agg_filter.get('op')
                    val = agg_filter.get('val')
                    if col not in projection_columns:
                        raise Exception(_('Aggregation column {} must be present in metrics.'.format(col)))
                    having_filter['$match']['$and'].append({col: {FILTER_MAP[op]: float(val)}})
                pipeline.append(having_filter)

        if not groupby and columns:
            metrics_exprs, pipeline = [], []
            column_exprs = {}
            for s in columns:
                column_exprs[s] = 1
            column_exprs['_id'] = 0
            pipeline.append(column_exprs)

        if not orderby and not columns:
            orderby = [(main_metric_expr, not order_desc)]

        if not columns:
            sort_exprs = {'$sort': {}}
            for col, ascending in orderby:
                sort_exprs['$sort'][col] = 1 if ascending else -1

            if sort_exprs['$sort']:
                pipeline.append(sort_exprs)

            if row_limit:
                pipeline.append({"$limit": row_limit})

        return pipeline

    def query(self, query_obj):
        qry_start_dttm = datetime.datetime.now()
        client = self.cluster.get_cosmos_client()
        pipeline = self.get_query_str(
            client=client, query_obj=query_obj, phase=2
        )
        if not query_obj.get('columns'):
            df = pd.DataFrame(list(client[self.cluster_name][self.datasource_name].aggregate(pipeline)))
        else:
            sort_exprs = []
            row_limit = query_obj.get('row_limit') or conf.get('ROW_LIMIT')
            for col, ascending in query_obj.get('orderby', []):
                sort_exprs.append((col, 1 if ascending else -1))
            if sort_exprs:
                df = pd.DataFrame(list(client[self.cluster_name][self.datasource_name]
                                       .find({}, pipeline[0])
                                       .sort(sort_exprs)
                                       .limit(row_limit)))
            else:
                df = pd.DataFrame(list(client[self.cluster_name][self.datasource_name]
                                       .find({}, pipeline[0])
                                       .limit(row_limit)))

        if df is None or df.size == 0:
            raise Exception(_('No data was returned.'))

        return QueryResult(
            df=df,
            query=str(pprint.pformat(pipeline, width=20, indent=2)),
            duration=datetime.datetime.now() - qry_start_dttm
        )