from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
import json
import logging

from flask import flash, Markup, redirect
from flask_appbuilder import CompactCRUDMixin, expose
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.security.decorators import has_access
from flask_babel import gettext as __
from flask_babel import lazy_gettext as _

from superset import appbuilder, db, security_manager, utils
from superset.connectors.base.views import DatasourceModelView
from superset.connectors.connector_registry import ConnectorRegistry
from superset.views.base import (
    BaseSupersetView, DatasourceFilter, DeleteMixin,
    get_datasource_exist_error_mgs, ListWidgetWithCheckboxes, SupersetModelView,
    validate_json, YamlExportMixin,
)
from . import models

class CosmosColumnInlineView(CompactCRUDMixin, SupersetModelView):
    datamodel = SQLAInterface(models.CosmosColumn)

    list_title = _('List Cosmos Column')
    show_title = _('Show Cosmos Column')
    add_title = _('Add Cosmos Column')
    edit_title = _('Edit Cosmos Column')

    list_widget = ListWidgetWithCheckboxes

    edit_columns = [
        'column_name', 'description', 'json', 'datasource',
        'groupby', 'filterable', 'count_distinct', 'sum', 'min', 'max',
    ]
    add_columns = edit_columns
    list_columns = [
        'column_name', 'verbose_name', 'type', 'groupby',
        'filterable', 'count_distinct', 'sum', 'min', 'max',
    ]
    can_delete = False
    page_size = 500
    label_columns = {
        'column_name': _('Column'),
        'type': _('Type'),
        'datasource': _('Datasource'),
        'groupby': _('Groupable'),
        'filterable': _('Filterable'),
        'count_distinct': _('Distinct'),
        'sum': _('Sum'),
        'min': _('Min'),
        'max': _('Max')
    }
    description_columns = {
        'filterable': _(
            'Whether this column is exposed in the `Filters` section '
            'of the explore view.'),
    }

    def post_update(self, col):
        col.refresh_metrics()

    def post_add(self, col):
        self.post_update(col)

appbuilder.add_view_no_menu(CosmosColumnInlineView)

class CosmosMetricInlineView(CompactCRUDMixin, SupersetModelView):
    datamodel = SQLAInterface(models.CosmosMetric)

    list_title = _('List Cosmos Metric')
    show_title = _('Show Cosmos Metric')
    add_title = _('Add Cosmos Metric')
    edit_title = _('Edit Cosmos Metric')

    list_columns = ['metric_name', 'verbose_name', 'metric_type']
    edit_columns = [
        'metric_name', 'description', 'verbose_name', 'metric_type', 'json',
        'datasource', 'd3format', 'is_restricted', 'warning_text'
    ]
    add_columns = edit_columns
    page_size = 500
    validators_columns = {
        'json': [validate_json],
    }
    description_columns = {
        'metric_type': utils.markdown(
            'use `postagg` as the metric type if you are defining a '
            '[Druid Post Aggregation]'
            '(http://druid.io/docs/latest/querying/post-aggregations.html)',
            True),
        'is_restricted': _('Whether the access to this metric is restricted '
                           'to certain roles. Only roles with the permission '
                           "'metric access on XXX (the name of this metric)' "
                           'are allowed to access this metric'),
    }
    label_columns = {
        'metric_name': _('Metric'),
        'description': _('Description'),
        'verbose_name': _('Verbose Name'),
        'metric_type': _('Type'),
        'json': _('JSON'),
        'datasource': _('Cosmos Datasource'),
        'warning_text': _('Warning Message'),
    }

    def post_add(self, metric):
        if metric.is_restricted:
            security_manager.merge_perm('metric_access', metric.get_perm())

    def post_update(self, metric):
        if metric.is_restricted:
            security_manager.merge_perm('metric_access', metric.get_perm())

appbuilder.add_view_no_menu(CosmosMetricInlineView)

class CosmosClusterModelView(SupersetModelView, DeleteMixin, YamlExportMixin):
    datamodel = SQLAInterface(models.CosmosCluster)

    list_title = _('List Cosmos Cluster')
    show_title = _('Show Cosmos Cluster')
    add_title = _('Add Cosmos Cluster')
    edit_title = _('Edit Cosmos Cluster')

    add_columns = [
        'verbose_name', 'cache_timeout', 'cluster_name',
        'mongo_url'
    ]
    edit_columns = add_columns
    list_columns = ['cluster_name', 'metadata_last_refreshed']
    search_columns = ('cluster_name', )
    label_columns = {
        'cluster_name': _('Cluster'),
        'mongo_url': _('Mongo DB url')
    }

    def pre_add(self, cluster):
        security_manager.merge_perm('database_access', cluster.perm)

    def pre_update(self, cluster):
        self.pre_add(cluster)

    def _delete(self, pk):
        DeleteMixin._delete(self, pk)

appbuilder.add_view(
    CosmosClusterModelView,
    name='Cosmos Clusters',
    label=__('Cosmos Clusters'),
    icon='fa-cubes',
    category='Sources',
    category_label=__('Sources'),
    category_icon='fa-database',
)


class CosmosDatasourceModelView(SupersetModelView, DeleteMixin, YamlExportMixin):
    datamodel = SQLAInterface(models.CosmosDatasource)

    list_title = _('List Cosmos Datasource')
    show_title = _('Show Cosmos Datasource')
    add_title = _('Add Cosmos Datasource')
    edit_title = _('Edit Cosmos Datasource')

    list_columns = [
        'datasource_link', 'cluster', 'changed_by_', 'modified'
    ]
    order_columns = ['datasource_link', 'modified']
    related_views = [CosmosColumnInlineView, CosmosMetricInlineView]
    edit_columns = [
        'datasource_name', 'cluster', 'slices', 'description',
        'owner', 'is_hidden', 'filter_select_enabled', 'fetch_values_from',
        'default_endpoint', 'offset', 'cache_timeout'
    ]
    search_columns = (
        'datasource_name', 'cluster', 'description', 'owner'
    )
    add_columns = edit_columns
    show_columns = add_columns + ['perm']
    page_size = 500
    base_order = ('datasource_name', 'asc')
    description_columns = {
        'slices': _(
            'The list of slices associated with this table. By '
            'altering this datasource, you may change how these associated '
            'slices behave. '
            'Also note that slices need to point to a datasource, so '
            'this form will fail at saving if removing slices from a '
            'datasource. If you want to change the datasource for a slice, '
            "overwrite the slice from the 'explore view'"),
        'offset': _('Timezone offset (in hours) for this datasource'),
        'description': Markup(
            'Supports <a href="'
            'https://daringfireball.net/projects/markdown/">markdown</a>'),
        'fetch_values_from': _(
            'Time expression to use as a predicate when retrieving '
            'distinct values to populate the filter component. '
            'Only applies when `Enable Filter Select` is on. If '
            'you enter `7 days ago`, the distinct list of values in '
            'the filter will be populated based on the distinct value over '
            'the past week'),
        'filter_select_enabled': _(
            "Whether to populate the filter's dropdown in the explore "
            "view's filter section with a list of distinct values fetched "
            'from the backend on the fly'),
        'default_endpoint': _(
            'Redirects to this endpoint when clicking on the datasource '
            'from the datasource list'),
    }
    base_filters = [['id', DatasourceFilter, lambda: []]]
    label_columns = {
        'slices': _('Associated Charts'),
        'datasource_link': _('Data Source'),
        'cluster': _('Cluster'),
        'description': _('Description'),
        'owner': _('Owner'),
        'is_hidden': _('Is Hidden'),
        'filter_select_enabled': _('Enable Filter Select'),
        'default_endpoint': _('Default Endpoint'),
        'offset': _('Time Offset'),
        'cache_timeout': _('Cache Timeout'),
    }

    def pre_add(self, datasource):
        with db.session.no_autoflush:
            query = (
                db.session.query(models.CosmosDatasource)
                .filter(models.CosmosDatasource.datasource_name ==
                        datasource.datasource_name,
                        models.CosmosDatasource.cluster_name ==
                        datasource.cluster.id)
            )
            if db.session.query(query.exists()).scalar():
                raise Exception(get_datasource_exist_error_mgs(
                    datasource.full_name))

    def post_add(self, datasource):
        datasource.refresh_metrics()
        security_manager.merge_perm('datasource_access', datasource.get_perm())
        if datasource.schema:
            security_manager.merge_perm('schema_access', datasource.schema_perm)

    def post_update(self, datasource):
        self.post_add(datasource)

    def _delete(self, pk):
        DeleteMixin._delete(self, pk)

appbuilder.add_view(
    CosmosDatasourceModelView,
    'Cosmos Datasources',
    label=__('Cosmos Datasources'),
    category='Sources',
    category_label=__('Sources'),
    icon='fa-cube')

class Cosmos(BaseSupersetView):

    @has_access
    @expose("/refresh_datasources/")
    def refresh_datasources(self, refreshAll=True):
        session = db.session()
        CosmosCluster = ConnectorRegistry.sources['cosmos'].cluster_class
        for cluster in session.query(CosmosCluster).all():
            cluster_name = cluster.cluster_name
            try:
                cluster.refresh_datasources(refreshAll=refreshAll)
            except Exception as e:
                flash(
                    "Error while processing cluster '{}'\n{}".format(
                        cluster_name, utils.error_msg_from_exception(e)
                    ),
                    'danger'
                )
                logging.exception(e)
                return redirect('/cosmosclustermodelview/list/')
            cluster.metadata_last_refreshed = datetime.now()
            flash(
                'Refreshed metadata from cluster '
                '[' + cluster.cluster_name + ']',
                'info'
            )
        session.commit()
        return redirect('/cosmosdatasourcemodelview/list/')

appbuilder.add_view_no_menu(Cosmos)

appbuilder.add_link(
    'Refresh Cosmos Datasources',
    label=__('Refresh Cosmos Metadata'),
    href='/cosmos/refresh_datasources/',
    category='Sources',
    category_label=__('Sources'),
    category_icon='fa-database',
    icon='fa-cog'
)

