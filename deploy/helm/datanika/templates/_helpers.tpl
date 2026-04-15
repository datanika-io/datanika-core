{{/*
Datanika Helm chart — shared template helpers.
*/}}

{{/* Full release name — truncated to 63 chars (k8s DNS label limit). */}}
{{- define "datanika.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* Chart base name for labels. */}}
{{- define "datanika.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Chart version label. */}}
{{- define "datanika.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Common labels applied to every rendered object. */}}
{{- define "datanika.labels" -}}
helm.sh/chart: {{ include "datanika.chart" . }}
{{ include "datanika.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Selector labels — immutable fields used by Deployments/StatefulSets. */}}
{{- define "datanika.selectorLabels" -}}
app.kubernetes.io/name: {{ include "datanika.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Service-specific selector (adds the component label). */}}
{{- define "datanika.componentSelector" -}}
{{ include "datanika.selectorLabels" . }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/* ServiceAccount name — either from values or generated. */}}
{{- define "datanika.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "datanika.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Postgres host — internal service or external override. */}}
{{- define "datanika.postgresHost" -}}
{{- if .Values.postgres.enabled }}
{{- printf "%s-postgres" (include "datanika.fullname" .) }}
{{- else }}
{{- required "externalDatabase.host is required when postgres.enabled=false" .Values.externalDatabase.host }}
{{- end }}
{{- end }}

{{/* Redis host — internal service or external override. */}}
{{- define "datanika.redisHost" -}}
{{- if .Values.redis.enabled }}
{{- printf "%s-redis" (include "datanika.fullname" .) }}
{{- else }}
{{- required "externalRedis.host is required when redis.enabled=false" .Values.externalRedis.host }}
{{- end }}
{{- end }}
