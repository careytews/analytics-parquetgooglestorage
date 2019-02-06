
//
// Definition for ParquetGoogleStorage loader
//

// Import KSonnet library.
local k = import "ksonnet.beta.2/k.libsonnet";
local tnw = import "lib/tnw-common.libsonnet";

// Short-cuts to various objects in the KSonnet library.
local depl = k.extensions.v1beta1.deployment;
local container = depl.mixin.spec.template.spec.containersType;
local mount = container.volumeMountsType;
local volume = depl.mixin.spec.template.spec.volumesType;
local resources = container.resourcesType;
local env = container.envType;
local secretDisk = volume.mixin.secret;
local annotations = depl.mixin.spec.template.metadata.annotations;

local worker(config) = {

    local version = import "version.jsonnet",

    name: "analytics-parquetgooglestorage",
    namespace: config.namespace,
    images: [config.containerBase + "/analytics-parquetgooglestorage:" + version],

    input: config.workers.queues.parquetgooglestorage.input,
    output: config.workers.queues.parquetgooglestorage.output,

    // Volumes - single volume containing the key secret
    volumeMounts:: [
        mount.new("keys", "/key") + mount.readOnly(true)
    ],

    // Environment variables
    envs:: [

	// AMQP Broker URL
        env.new("AMQP_BROKER", "amqp://guest:guest@amqp:5672/"),
     
        // Pathname of key file.
        env.new("KEY", "/key/private.json"),

        // Googlestorage table settings
        env.new("GS_PROJECT", config.project),
        env.new("GS_BUCKET", config.parquet_bucket),
        env.new("GS_BASEDIR", config.parquet_basedir),
        env.new("MAX_BATCH", "256M"),
        env.new("MAX_TIME", "1800")

    ],

    // Container definition.
    containers:: [
        container.new(self.name, self.images[0]) +
            container.volumeMounts(self.volumeMounts) +
            container.env(self.envs) +
            container.args([self.input] +
                           std.map(function(x) "output:" + x,
                                   self.output)) +
            container.mixin.resources.limits({
                memory: "1.5G", cpu: "1.25"
            }) +
            container.mixin.resources.requests({
                memory: "1.5G", cpu: "1.2"
            })
    ],

    // Volumes
    volumes:: [
        volume.name("keys") +
            secretDisk.secretName("analytics-parquet-keys")
    ],

    // Deployment definition.
    deployments:: [
    depl.new(self.name,
                 config.workers.replicas.parquetgooglestorage.min,
                 self.containers,
                 {app: "analytics-parquetgooglestorage",
                  component: "analytics"}) +
  			depl.mixin.metadata.namespace($.namespace) +
            depl.mixin.spec.template.spec.volumes(self.volumes) +
	annotations({"prometheus.io/scrape": "true",
		     "prometheus.io/port": "8080"})
    ],

    resources:: 
		if config.options.includeAnalytics then
			self.deployments
		else [],
};

[worker]
