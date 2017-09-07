import React, { Component } from 'react';

import Sidebar from './sidebar';
import ServiceTable from './serviceTable';

const userOptions = ['david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2'];
const metricGroupOptions = ['david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2','david', 'group1', 'group2', 'role1', 'role2'];


const fields = [
  {
    id: 'name',
    label: 'Name',
    numeric: false,
  },
  {
    id: 'runAs',
    label: 'Run As',
    numeric: false,
  },
  {
    id: 'cluster',
    label: 'Cluster',
    numeric: false,
  },
  {
    id: 'cpus',
    label: 'CPUs',
    numeric: true,
  },
  {
    id: 'memory',
    label: 'Memory',
    numeric: true,
  },
  {
    id: 'version',
    label: 'Version',
    numeric: false,
  },
  {
    id: 'status',
    label: 'Status',
    numeric: false,
  },
];

const services_ = [
  {
    name: 'Service A',
    cluster: 'cA',
    runAs: 'david',
    cpus: 10,
    memory: '10 MB',
    version: 'A',
    status: 'A',
  },
  {
    name: 'Service Name B',
    cluster: 'cB',
    runAs: 'david',
    cpus: 10,
    memory: '10 MB',
    version: 'A',
    status: 'A',
  },
  {
    name: 'Service A',
    cluster: 'cA',
    runAs: 'david',
    cpus: 10,
    memory: '10 MB',
    version: 'A',
    status: 'A',
  },
  {
    name: 'Service A',
    cluster: 'cB',
    runAs: 'david medina',
    cpus: 10,
    memory: '10 MB',
    version: 'A',
    status: 'A',
  },
];

const services = [
  ...services_,
  ...services_,
  ...services_,
  ...services_,
  ...services_,
  ...services_,
  ...services_,
  ...services_,
  ...services_,
].map((data, id) => (
  { ...data, id }
));

class Services extends Component {
  constructor(props) {
    super(props);
    this.state = {
      clusters: {
        ClusterA: true,
        ClusterB: true,
        ClusterC: false,
        ClusterD: true,
      }
    };
  }

  onClusterClick = (cluster) => {
    const { clusters } = this.state;
    this.setState({
      clusters: {
        ...clusters,
        [cluster]: !clusters[cluster],
      },
    });
  };

  render() {
    const { classes } = this.props;
    return (
      <div>
        <Sidebar
          user="david"
          userOptions={userOptions}
          metricGroup="david"
          metricGroupOptions={metricGroupOptions}
          clusters={this.state.clusters}
          onClusterClick={this.onClusterClick}
        />
        <div
          style={{
            height: 'calc(100vh - 70px)',
            width: 'calc(100% - 200px)',
            marginLeft: 200,
            padding: 0,
          }}
        >
          <ServiceTable
            fields={fields}
            services={services}
          />
        </div>
      </div>
    );
  }
};

export default Services;
