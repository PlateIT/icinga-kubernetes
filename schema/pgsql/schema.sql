CREATE TABLE cluster (
  uuid bytea NOT NULL,
  name varchar(255) NULL DEFAULT NULL,
  CONSTRAINT pk_cluster PRIMARY KEY (uuid)
);

CREATE TABLE annotation (
  uuid bytea NOT NULL,
  name varchar(317) NOT NULL,
  value bytea NOT NULL,
  CONSTRAINT pk_annotation PRIMARY KEY (uuid)
);

CREATE TABLE resource_annotation (
  resource_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_resource_annotation PRIMARY KEY (resource_uuid, annotation_uuid)
);

CREATE TABLE label (
  uuid bytea NOT NULL,
  name varchar(317) NOT NULL,
  value varchar(255) NOT NULL,
  CONSTRAINT pk_label PRIMARY KEY (uuid)
);

CREATE TABLE resource_label (
  resource_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_resource_label PRIMARY KEY (resource_uuid, label_uuid)
);

CREATE TABLE config_map (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  immutable varchar(255) NOT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_config_map PRIMARY KEY (uuid)
);

CREATE TABLE config_map_annotation (
  config_map_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_config_map_annotation PRIMARY KEY (config_map_uuid, annotation_uuid)
);

CREATE TABLE config_map_label (
  config_map_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_config_map_label PRIMARY KEY (config_map_uuid, label_uuid)
);

CREATE TABLE container (
  uuid bytea NOT NULL,
  pod_uuid bytea NOT NULL,
  name varchar(255) NOT NULL,
  image varchar(512) NOT NULL,
  image_pull_policy varchar(255) NULL DEFAULT NULL,
  cpu_limits bigint NULL DEFAULT NULL,
  cpu_requests bigint NULL DEFAULT NULL,
  memory_limits bigint NULL DEFAULT NULL,
  memory_requests bigint NULL DEFAULT NULL,
  state varchar(255) NULL DEFAULT NULL,
  state_details text NULL DEFAULT NULL,
  ready varchar(255) NOT NULL,
  started varchar(255) NOT NULL,
  restart_count integer NOT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NULL DEFAULT NULL,
  CONSTRAINT pk_container PRIMARY KEY (uuid)
);

CREATE TABLE init_container (
  uuid bytea NOT NULL,
  pod_uuid bytea NOT NULL,
  name varchar(255) NOT NULL,
  image varchar(512) NOT NULL,
  image_pull_policy varchar(255) NULL DEFAULT NULL,
  cpu_limits bigint NULL DEFAULT NULL,
  cpu_requests bigint NULL DEFAULT NULL,
  memory_limits bigint NULL DEFAULT NULL,
  memory_requests bigint NULL DEFAULT NULL,
  state varchar(255) NULL DEFAULT NULL,
  state_details text NULL DEFAULT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NULL DEFAULT NULL,
  CONSTRAINT pk_init_container PRIMARY KEY (uuid)
);

CREATE TABLE sidecar_container (
  uuid bytea NOT NULL,
  pod_uuid bytea NOT NULL,
  name varchar(255) NOT NULL,
  image varchar(512) NOT NULL,
  image_pull_policy varchar(255) NULL DEFAULT NULL,
  cpu_limits bigint NULL DEFAULT NULL,
  cpu_requests bigint NULL DEFAULT NULL,
  memory_limits bigint NULL DEFAULT NULL,
  memory_requests bigint NULL DEFAULT NULL,
  state varchar(255) NULL DEFAULT NULL,
  state_details text NULL DEFAULT NULL,
  ready varchar(255) NOT NULL,
  started varchar(255) NOT NULL,
  restart_count integer NOT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NULL DEFAULT NULL,
  CONSTRAINT pk_sidecar_container PRIMARY KEY (uuid)
);

CREATE TABLE container_device (
  container_uuid bytea NOT NULL,
  pod_uuid bytea NOT NULL,
  name varchar(253) NOT NULL,
  path varchar(255) NOT NULL,
  CONSTRAINT pk_container_device PRIMARY KEY (container_uuid, name)
);

CREATE TABLE container_log (
  container_uuid bytea NOT NULL,
  pod_uuid bytea NOT NULL,
  logs text NOT NULL,
  last_update bigint NOT NULL,
  CONSTRAINT pk_container_log PRIMARY KEY (container_uuid)
);

CREATE TABLE container_mount (
  container_uuid bytea NOT NULL,
  pod_uuid bytea NOT NULL,
  volume_name varchar(255) NOT NULL,
  path varchar(255) NOT NULL,
  sub_path varchar(255) NULL DEFAULT NULL,
  read_only varchar(255) NOT NULL,
  CONSTRAINT pk_container_mount PRIMARY KEY (container_uuid, volume_name)
);

CREATE TABLE cron_job (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  schedule varchar(255) NOT NULL,
  timezone varchar(255) NULL DEFAULT NULL,
  starting_deadline_seconds bigint NULL DEFAULT NULL,
  concurrency_policy varchar(255) NOT NULL,
  suspend varchar(255) NOT NULL,
  successful_jobs_history_limit integer NOT NULL,
  failed_jobs_history_limit integer NOT NULL,
  active integer NOT NULL,
  last_schedule_time bigint NULL DEFAULT NULL,
  last_successful_time bigint NULL DEFAULT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NOT NULL,
  yaml bytea DEFAULT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_cron_job PRIMARY KEY (uuid)
);

CREATE TABLE cron_job_annotation (
  cron_job_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_cron_job_annotation PRIMARY KEY (cron_job_uuid, annotation_uuid)
);

CREATE TABLE cron_job_label (
  cron_job_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_cron_job_label PRIMARY KEY (cron_job_uuid, label_uuid)
);

CREATE TABLE daemon_set (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  update_strategy varchar(255) NOT NULL,
  min_ready_seconds integer NOT NULL,
  desired_number_scheduled integer NOT NULL,
  current_number_scheduled integer NOT NULL,
  number_misscheduled integer NOT NULL,
  number_ready integer NOT NULL,
  update_number_scheduled integer NOT NULL,
  number_available integer NOT NULL,
  number_unavailable integer NOT NULL,
  yaml bytea DEFAULT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NOT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_daemon_set PRIMARY KEY (uuid)
);

CREATE TABLE daemon_set_annotation (
  daemon_set_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_daemon_set_annotation PRIMARY KEY (daemon_set_uuid, annotation_uuid)
);

CREATE TABLE daemon_set_condition (
  daemon_set_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  last_transition bigint NOT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_daemon_set_condition PRIMARY KEY (daemon_set_uuid, type)
);

CREATE TABLE daemon_set_label (
  daemon_set_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_daemon_set_label PRIMARY KEY (daemon_set_uuid, label_uuid)
);

CREATE TABLE daemon_set_owner (
  daemon_set_uuid bytea NOT NULL,
  owner_uuid bytea NOT NULL,
  kind varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  controller varchar(255) NOT NULL,
  block_owner_deletion varchar(255) NOT NULL,
  CONSTRAINT pk_daemon_set_owner PRIMARY KEY (daemon_set_uuid, owner_uuid)
);

CREATE TABLE deployment (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255)  NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  strategy varchar(255) NOT NULL,
  min_ready_seconds integer NOT NULL,
  progress_deadline_seconds integer NOT NULL,
  paused varchar(255) NOT NULL,
  desired_replicas integer NOT NULL,
  actual_replicas integer NOT NULL,
  updated_replicas integer NOT NULL,
  ready_replicas integer NOT NULL,
  available_replicas integer NOT NULL,
  unavailable_replicas integer NOT NULL,
  yaml bytea DEFAULT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NOT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_deployment PRIMARY KEY (uuid)
);

CREATE TABLE deployment_annotation (
  deployment_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_deployment_annotation PRIMARY KEY (deployment_uuid, annotation_uuid)
);

CREATE TABLE deployment_condition (
  deployment_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  last_update bigint NOT NULL,
  last_transition bigint NOT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_deployment_condition PRIMARY KEY (deployment_uuid, type)
);

CREATE TABLE deployment_label (
  deployment_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_deployment_label PRIMARY KEY (deployment_uuid, label_uuid)
);

CREATE TABLE deployment_owner (
  deployment_uuid bytea NOT NULL,
  owner_uuid bytea NOT NULL,
  kind varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  controller varchar(255) NOT NULL,
  block_owner_deletion varchar(255) NOT NULL,
  CONSTRAINT pk_deployment_owner PRIMARY KEY (deployment_uuid, owner_uuid)
);

CREATE TABLE endpoint (
  uuid bytea NOT NULL,
  endpoint_slice_uuid bytea NOT NULL,
  host_name varchar(253) NOT NULL,
  node_name varchar(253) NOT NULL,
  ready varchar(255) NULL DEFAULT NULL,
  serving varchar(255) NULL DEFAULT NULL,
  terminating varchar(255) NULL DEFAULT NULL,
  address varchar(253) NOT NULL,
  protocol varchar(255) NOT NULL,
  port integer NOT NULL,
  port_name varchar(253) NOT NULL,
  app_protocol varchar(253) NOT NULL,
  CONSTRAINT pk_endpoint PRIMARY KEY (uuid)
);

CREATE TABLE endpoint_slice (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  address_type varchar(255) NOT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_endpoint_slice PRIMARY KEY (uuid)
);

CREATE TABLE endpoint_slice_label (
  endpoint_slice_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_endpoint_slice_label PRIMARY KEY (endpoint_slice_uuid, label_uuid)
);

CREATE TABLE endpoint_target_ref (
  endpoint_slice_uuid bytea NOT NULL,
  kind varchar(255) NULL DEFAULT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  uid varchar(255) NOT NULL,
  api_version varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  CONSTRAINT pk_endpoint_target_ref PRIMARY KEY (endpoint_slice_uuid)
);

CREATE TABLE event (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  reference_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(270) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  reporting_controller varchar(253) NULL DEFAULT NULL,
  reporting_instance varchar(128) NULL DEFAULT NULL,
  action varchar(128) NULL DEFAULT NULL,
  reason varchar(128) NOT NULL,
  note text NOT NULL,
  type varchar(255) NOT NULL,
  reference_kind varchar(255) NOT NULL,
  reference_namespace varchar(255) NULL DEFAULT NULL,
  reference_name varchar(253) NOT NULL,
  first_seen bigint NOT NULL,
  last_seen bigint NOT NULL,
  count integer NOT NULL,
  yaml bytea DEFAULT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_event PRIMARY KEY (uuid)
);


CREATE TABLE ingress (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  yaml bytea DEFAULT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_ingress PRIMARY KEY (uuid)
);

CREATE TABLE ingress_annotation (
  ingress_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_ingress_annotation PRIMARY KEY (ingress_uuid, annotation_uuid)
);

CREATE TABLE ingress_backend_resource (
  resource_uuid bytea NOT NULL,
  ingress_uuid bytea NOT NULL,
  ingress_rule_uuid bytea NULL DEFAULT NULL,
  api_group varchar(255) NULL DEFAULT NULL,
  kind varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  CONSTRAINT pk_ingress_backend_resource PRIMARY KEY (resource_uuid, ingress_uuid)
);

CREATE TABLE ingress_backend_service (
  service_uuid bytea NOT NULL,
  ingress_uuid bytea NOT NULL,
  ingress_rule_uuid bytea NULL DEFAULT NULL,
  service_name varchar(255) NOT NULL,
  service_port_name varchar(255) NULL DEFAULT NULL,
  service_port_number integer NULL DEFAULT NULL,
  CONSTRAINT pk_ingress_backend_service PRIMARY KEY (service_uuid, ingress_uuid)
);

CREATE TABLE ingress_label (
  ingress_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_ingress_label PRIMARY KEY (ingress_uuid, label_uuid)
);

CREATE TABLE ingress_rule (
  uuid bytea NOT NULL,
  backend_uuid bytea NOT NULL,
  ingress_uuid bytea NOT NULL,
  host varchar(255) NULL DEFAULT NULL,
  path varchar(255) NULL DEFAULT NULL,
  path_type varchar(255) NOT NULL,
  CONSTRAINT pk_ingress_rule PRIMARY KEY (uuid)
);

CREATE TABLE ingress_tls (
  ingress_uuid bytea NOT NULL,
  tls_host varchar(255) NOT NULL,
  tls_secret varchar(255) NULL DEFAULT NULL,
  CONSTRAINT pk_ingress_tls PRIMARY KEY (ingress_uuid)
);

CREATE TABLE job (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  parallelism integer NULL DEFAULT NULL,
  completions integer NULL DEFAULT NULL,
  active_deadline_seconds bigint NULL DEFAULT NULL,
  backoff_limit integer NULL DEFAULT NULL,
  ttl_seconds_after_finished integer NULL DEFAULT NULL,
  completion_mode varchar(255) NULL DEFAULT NULL,
  suspend varchar(255) NOT NULL,
  start_time bigint NULL DEFAULT NULL,
  completion_time bigint NULL DEFAULT NULL,
  active integer NOT NULL,
  succeeded integer NOT NULL,
  failed integer NOT NULL,
  yaml bytea DEFAULT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NOT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_job PRIMARY KEY (uuid)
);

CREATE TABLE job_annotation (
  job_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_job_annotation PRIMARY KEY (job_uuid, annotation_uuid)
);

CREATE TABLE job_condition (
  job_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  last_probe bigint NULL DEFAULT NULL,
  last_transition bigint NOT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_job_condition PRIMARY KEY (job_uuid, type)
);

CREATE TABLE job_label (
  job_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_job_label PRIMARY KEY (job_uuid, label_uuid)
);

CREATE TABLE job_owner (
  job_uuid bytea NOT NULL,
  owner_uuid bytea NOT NULL,
  kind varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  controller varchar(255) NOT NULL,
  block_owner_deletion varchar(255) NOT NULL,
  CONSTRAINT pk_job_owner PRIMARY KEY (job_uuid, owner_uuid)
);

CREATE TABLE namespace (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL, /* TODO: Remove. A namespace does not have a namespace. */
  name varchar(255) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  phase varchar(255) NOT NULL,
  yaml bytea DEFAULT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_namespace PRIMARY KEY (uuid)
);

CREATE TABLE namespace_annotation (
  namespace_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_namespace_annotation PRIMARY KEY (namespace_uuid, annotation_uuid)
);

CREATE TABLE namespace_condition (
  namespace_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  last_transition bigint NOT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_namespace_condition PRIMARY KEY (namespace_uuid, type)
);

CREATE TABLE namespace_label (
  namespace_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_namespace_label PRIMARY KEY (namespace_uuid, label_uuid)
);

CREATE TABLE node (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  pod_cidr varchar(255) NOT NULL,
  num_ips integer NOT NULL,
  unschedulable varchar(255) NOT NULL,
  ready varchar(255) NOT NULL,
  cpu_capacity bigint NOT NULL,
  cpu_allocatable bigint NOT NULL,
  memory_capacity bigint NOT NULL,
  memory_allocatable bigint NOT NULL,
  pod_capacity integer NOT NULL,
  yaml bytea DEFAULT NULL,
  roles varchar(255) NOT NULL,
  machine_id varchar(255) NOT NULL,
  system_uuid varchar(255) NOT NULL,
  boot_id varchar(255) NOT NULL,
  kernel_version varchar(255) NOT NULL,
  os_image varchar(512) NOT NULL,
  operating_system varchar(255) NOT NULL,
  architecture varchar(255) NOT NULL,
  container_runtime_version varchar(255) NOT NULL,
  kubelet_version varchar(255) NOT NULL,
  kube_proxy_version varchar(255) NOT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NOT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_node PRIMARY KEY (uuid)
);

CREATE TABLE node_annotation (
  node_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_node_annotation PRIMARY KEY (node_uuid, annotation_uuid)
);

CREATE TABLE node_condition (
  node_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  last_heartbeat bigint NOT NULL,
  last_transition bigint NOT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_node_condition PRIMARY KEY (node_uuid, type)
);

CREATE TABLE node_label (
  node_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_node_label PRIMARY KEY (node_uuid, label_uuid)
);

CREATE TABLE node_volume (
  node_uuid bytea NOT NULL,
  name varchar(253) NOT NULL,
  device_path varchar(255) NOT NULL,
  mounted varchar(255) NOT NULL,
  CONSTRAINT pk_node_volume PRIMARY KEY (node_uuid, name)
);

CREATE TABLE persistent_volume (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  capacity bigint NOT NULL,
  phase varchar(255) NOT NULL,
  reason varchar(255) NULL DEFAULT NULL,
  message text NULL DEFAULT NULL,
  access_modes smallint NULL DEFAULT NULL,
  volume_mode varchar(255) NOT NULL,
  volume_source_type varchar(255) NOT NULL,
  storage_class varchar(255) NULL DEFAULT NULL,
  volume_source text NOT NULL,
  reclaim_policy varchar(255) NOT NULL,
  yaml bytea DEFAULT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_persistent_volume PRIMARY KEY (uuid)
);

CREATE TABLE persistent_volume_annotation (
  persistent_volume_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_persistent_volume_annotation PRIMARY KEY (persistent_volume_uuid, annotation_uuid)
);

CREATE TABLE persistent_volume_claim_ref (
  persistent_volume_uuid bytea NOT NULL,
  kind varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  CONSTRAINT pk_persistent_volume_claim_ref PRIMARY KEY (persistent_volume_uuid, uid)
);

CREATE TABLE persistent_volume_label (
  persistent_volume_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_persistent_volume_label PRIMARY KEY (persistent_volume_uuid, label_uuid)
);

CREATE TABLE pod (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  node_name varchar(253) NULL DEFAULT NULL,
  nominated_node_name varchar(253) NULL DEFAULT NULL,
  ip varchar(255) NULL DEFAULT NULL,
  restart_policy varchar(255) NOT NULL,
  cpu_limits bigint NULL DEFAULT NULL,
  cpu_requests bigint NULL DEFAULT NULL,
  memory_limits bigint NULL DEFAULT NULL,
  memory_requests bigint NULL DEFAULT NULL,
  phase varchar(255) NOT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NULL DEFAULT NULL,
  reason varchar(255) NULL DEFAULT NULL,
  message text NULL DEFAULT NULL,
  qos varchar(255) NULL DEFAULT NULL,
  yaml bytea DEFAULT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_pod PRIMARY KEY (uuid)
);

CREATE TABLE pod_annotation (
  pod_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_pod_annotation PRIMARY KEY (pod_uuid, annotation_uuid)
);

CREATE TABLE pod_condition (
  pod_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  last_probe bigint NULL DEFAULT NULL,
  last_transition bigint NOT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_pod_condition PRIMARY KEY (pod_uuid, type)
);

CREATE TABLE pod_label (
  pod_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_pod_label PRIMARY KEY (pod_uuid, label_uuid)
);

CREATE TABLE pod_metrics (
  namespace varchar(255) NOT NULL,
  pod_name varchar(253) NOT NULL,
  container_name varchar(255) NOT NULL,
  timestamp bigint NOT NULL,
  duration bigint NOT NULL,
  cpu_usage double precision NOT NULL,
  memory_usage double precision NOT NULL,
  storage_usage double precision NOT NULL,
  ephemeral_storage_usage double precision NOT NULL,
  CONSTRAINT pk_pod_metrics PRIMARY KEY (namespace, pod_name)
);

CREATE TABLE pod_owner (
  pod_uuid bytea NOT NULL,
  owner_uuid bytea NOT NULL,
  kind varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  controller varchar(255) NOT NULL,
  block_owner_deletion varchar(255) NOT NULL,
  CONSTRAINT pk_pod_owner PRIMARY KEY (pod_uuid, owner_uuid)
);

CREATE TABLE pod_pvc (
  pod_uuid bytea NOT NULL,
  volume_name varchar(253) NOT NULL,
  claim_name varchar(253) NOT NULL,
  read_only varchar(255) NOT NULL,
  CONSTRAINT pk_pod_pvc PRIMARY KEY (pod_uuid, volume_name, claim_name)
);

CREATE TABLE pod_volume (
  pod_uuid bytea NOT NULL,
  volume_name varchar(255) NOT NULL,
  type varchar(255) NOT NULL,
  source text NOT NULL,
  CONSTRAINT pk_pod_volume PRIMARY KEY (pod_uuid, volume_name)
);

CREATE TABLE prometheus_cluster_metric (
  cluster_uuid bytea NOT NULL,
  timestamp bigint NOT NULL,
  category varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  value double precision NOT NULL,
  CONSTRAINT pk_prometheus_cluster_metric PRIMARY KEY (cluster_uuid, timestamp, category, name)
);

CREATE TABLE prometheus_container_metric (
  container_uuid bytea NOT NULL,
  timestamp bigint NOT NULL,
  category varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  value double precision NOT NULL,
  CONSTRAINT pk_prometheus_container_metric PRIMARY KEY (container_uuid, timestamp, category, name)
);

CREATE TABLE prometheus_node_metric (
  node_uuid bytea NOT NULL,
  timestamp bigint NOT NULL,
  category varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  value double precision NOT NULL,
  CONSTRAINT pk_prometheus_node_metric PRIMARY KEY (node_uuid, timestamp, category, name)
);

CREATE TABLE prometheus_pod_metric (
  pod_uuid bytea NOT NULL,
  timestamp bigint NOT NULL,
  category varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  value double precision NOT NULL,
  CONSTRAINT pk_prometheus_pod_metric PRIMARY KEY (pod_uuid, timestamp, category, name)
);

CREATE TABLE pvc (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  desired_access_modes smallint NOT NULL,
  actual_access_modes smallint NULL DEFAULT NULL,
  minimum_capacity bigint NULL DEFAULT NULL,
  actual_capacity bigint NULL DEFAULT NULL,
  phase varchar(255) NOT NULL,
  volume_name varchar(253) NULL DEFAULT NULL,
  volume_mode varchar(255) NULL DEFAULT NULL,
  storage_class varchar(255) NULL DEFAULT NULL,
  yaml bytea DEFAULT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_pvc PRIMARY KEY (uuid)
);

CREATE TABLE pvc_annotation (
  pvc_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_pvc_annotation PRIMARY KEY (pvc_uuid, annotation_uuid)
);

CREATE TABLE pvc_condition (
  pvc_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  last_probe bigint NULL DEFAULT NULL,
  last_transition bigint NOT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_pvc_condition PRIMARY KEY (pvc_uuid, type)
);

CREATE TABLE pvc_label (
  pvc_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_pvc_label PRIMARY KEY (pvc_uuid, label_uuid)
);

CREATE TABLE replica_set (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  desired_replicas integer NOT NULL,
  min_ready_seconds integer NOT NULL,
  actual_replicas integer NOT NULL,
  fully_labeled_replicas integer NOT NULL,
  ready_replicas integer NOT NULL,
  available_replicas integer NOT NULL,
  yaml bytea DEFAULT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NOT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_replica_set PRIMARY KEY (uuid)
);

CREATE TABLE replica_set_annotation (
  replica_set_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_replica_set_annotation PRIMARY KEY (replica_set_uuid, annotation_uuid)
);

CREATE TABLE replica_set_condition (
  replica_set_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  last_transition bigint NOT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_replica_set_condition PRIMARY KEY (replica_set_uuid, type)
);

CREATE TABLE replica_set_label (
  replica_set_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_replica_set_label PRIMARY KEY (replica_set_uuid, label_uuid)
);

CREATE TABLE replica_set_owner (
  replica_set_uuid bytea NOT NULL,
  owner_uuid bytea NOT NULL,
  kind varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  controller varchar(255) NOT NULL,
  block_owner_deletion varchar(255) NOT NULL,
  CONSTRAINT pk_replica_set_owner PRIMARY KEY (replica_set_uuid, owner_uuid)
);

CREATE TABLE secret (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  type varchar(255) NOT NULL,
  immutable varchar(255) NOT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_secret PRIMARY KEY (uuid)
);

CREATE TABLE secret_annotation (
  secret_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_secret_annotation PRIMARY KEY (secret_uuid, annotation_uuid)
);

CREATE TABLE secret_label (
  secret_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_secret_label PRIMARY KEY (secret_uuid, label_uuid)
);

CREATE TABLE selector (
  uuid bytea NOT NULL,
  name varchar(317) NOT NULL,
  value varchar(255) NOT NULL,
  CONSTRAINT pk_selector PRIMARY KEY (uuid)
);

CREATE TABLE service (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  cluster_ip varchar(255) NOT NULL,
  cluster_ips varchar(255) NOT NULL,
  type varchar(255) NOT NULL,
  external_ips varchar(255) NULL DEFAULT NULL,
  session_affinity varchar(255) NOT NULL,
  external_name varchar(255) NULL DEFAULT NULL,
  external_traffic_policy varchar(255) NULL DEFAULT NULL,
  health_check_node_port integer NULL DEFAULT NULL,
  publish_not_ready_addresses varchar(255) NOT NULL,
  ip_families varchar(255) NULL DEFAULT NULL,
  ip_family_policy varchar(255) NULL DEFAULT NULL,
  allocate_load_balancer_node_ports varchar(255) NOT NULL,
  load_balancer_class varchar(255) NULL DEFAULT NULL,
  internal_traffic_policy varchar(255) NOT NULL,
  yaml bytea DEFAULT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_service PRIMARY KEY (uuid)
);

CREATE TABLE service_annotation (
  service_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_service_annotation PRIMARY KEY (service_uuid, annotation_uuid)
);

CREATE TABLE service_condition (
  service_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  observed_generation bigint NULL DEFAULT NULL,
  last_transition bigint NULL DEFAULT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_service_condition PRIMARY KEY (service_uuid, type)
);

CREATE TABLE service_label (
  service_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_service_label PRIMARY KEY (service_uuid, label_uuid)
);

CREATE TABLE service_pod (
  service_uuid bytea NOT NULL,
  pod_uuid bytea NOT NULL,
  CONSTRAINT pk_service_pod PRIMARY KEY (service_uuid, pod_uuid)
);

CREATE TABLE service_port (
  service_uuid bytea NOT NULL,
  name varchar(255) NOT NULL,
  protocol varchar(255) NOT NULL,
  app_protocol varchar(255) NOT NULL,
  port integer NOT NULL,
  target_port varchar(15) NOT NULL,
  node_port integer NOT NULL,
  CONSTRAINT pk_service_port PRIMARY KEY (service_uuid, name)
);

CREATE TABLE service_selector (
  service_uuid bytea NOT NULL,
  selector_uuid bytea NOT NULL,
  CONSTRAINT pk_service_selector PRIMARY KEY (service_uuid, selector_uuid)
);

CREATE TABLE stateful_set (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  namespace varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  resource_version varchar(255) NOT NULL,
  desired_replicas integer NOT NULL,
  service_name varchar(253) NOT NULL,
  pod_management_policy varchar(255) NOT NULL,
  update_strategy varchar(255) NOT NULL,
  min_ready_seconds integer NOT NULL,
  persistent_volume_claim_retention_policy_when_deleted varchar(255) NOT NULL,
  persistent_volume_claim_retention_policy_when_scaled varchar(255) NOT NULL,
  ordinals integer NOT NULL,
  actual_replicas integer NOT NULL,
  ready_replicas integer NOT NULL,
  current_replicas integer NOT NULL,
  updated_replicas integer NOT NULL,
  available_replicas integer NOT NULL,
  yaml bytea DEFAULT NULL,
  icinga_state varchar(255) NOT NULL,
  icinga_state_reason text NOT NULL,
  created bigint NOT NULL,
  CONSTRAINT pk_stateful_set PRIMARY KEY (uuid)
);

CREATE TABLE stateful_set_annotation (
  stateful_set_uuid bytea NOT NULL,
  annotation_uuid bytea NOT NULL,
  CONSTRAINT pk_stateful_set_annotation PRIMARY KEY (stateful_set_uuid, annotation_uuid)
);

CREATE TABLE stateful_set_condition (
  stateful_set_uuid bytea NOT NULL,
  type varchar(255) NOT NULL,
  status varchar(255) NOT NULL,
  last_transition bigint NOT NULL,
  reason varchar(255) NOT NULL,
  message text,
  CONSTRAINT pk_stateful_set_condition PRIMARY KEY (stateful_set_uuid, type)
);

CREATE TABLE stateful_set_label (
  stateful_set_uuid bytea NOT NULL,
  label_uuid bytea NOT NULL,
  CONSTRAINT pk_stateful_set_label PRIMARY KEY (stateful_set_uuid, label_uuid)
);

CREATE TABLE stateful_set_owner (
  stateful_set_uuid bytea NOT NULL,
  owner_uuid bytea NOT NULL,
  kind varchar(255) NOT NULL,
  name varchar(253) NOT NULL,
  uid varchar(255) NOT NULL,
  controller varchar(255) NOT NULL,
  block_owner_deletion varchar(255) NOT NULL,
  CONSTRAINT pk_stateful_set_owner PRIMARY KEY (stateful_set_uuid, owner_uuid)
);

CREATE TABLE favorite (
  resource_uuid bytea NOT NULL,
  kind varchar(255) NOT NULL,
  username varchar(254) NOT NULL,
  priority integer,
  CONSTRAINT pk_favorite PRIMARY KEY (resource_uuid, username)
);

CREATE INDEX idx_favorite_username ON favorite(username, kind);

CREATE TABLE kubernetes_instance (
  uuid bytea NOT NULL,
  cluster_uuid bytea NOT NULL,
  version varchar(255) NOT NULL,
  kubernetes_version varchar(255) NOT NULL,
  kubernetes_heartbeat bigint NULL DEFAULT NULL,
  kubernetes_api_reachable varchar(255) NOT NULL,
  message text NULL DEFAULT NULL,
  heartbeat bigint NOT NULL,
  CONSTRAINT pk_kubernetes_instance PRIMARY KEY (uuid)
);

CREATE TABLE config (
  cluster_uuid bytea NOT NULL,
  "key" varchar(255) NOT NULL,
  value varchar(255) NOT NULL,
  locked varchar(255) NOT NULL,

  CONSTRAINT pk_config PRIMARY KEY ("key", cluster_uuid)
);

CREATE TABLE kubernetes_schema (
  id integer GENERATED BY DEFAULT AS IDENTITY NOT NULL,
  version varchar(255) NOT NULL,
  timestamp bigint NOT NULL,
  success varchar(255) DEFAULT NULL,
  reason text DEFAULT NULL,
  CONSTRAINT pk_kubernetes_schema PRIMARY KEY (id),
  CONSTRAINT idx_kubernetes_schema_version UNIQUE (version)
);

INSERT INTO kubernetes_schema (version, timestamp, success, reason)
VALUES ('0.4.0', (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint, 'y', 'Initial import');


CREATE INDEX idx_event_created ON event(created);
CREATE INDEX idx_prometheus_cluster_metric_timestamp ON prometheus_cluster_metric(timestamp);
CREATE INDEX idx_prometheus_container_metric_timestamp ON prometheus_container_metric(timestamp);
CREATE INDEX idx_prometheus_node_metric_timestamp ON prometheus_node_metric(timestamp);
CREATE INDEX idx_prometheus_pod_metric_timestamp ON prometheus_pod_metric(timestamp);
