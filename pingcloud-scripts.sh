#!/bin/bash

# pingcloud-scripts::source_script - Sources a script either from local path or S3
# Arguments:
# $1 - script_name (e.g., k8s_utils, bash_utils)
# $2 - version (e.g., pdo-9231 or 1.3.0)
# $3 - aws_profile (optional)

pingcloud-scripts::source_script() {
    local script_name="${1}"
    local version="${2:-pdo-9231}"
    local aws_profile="${3:-${AWS_PROFILE}}"
    local usage="pingcloud-scripts::source_script SCRIPT_NAME VERSION [aws_profile]"

    echo "[DEBUG] Starting pingcloud-scripts::source_script for script: ${script_name}"
    echo "[DEBUG] Version set to: ${version}"
    echo "[DEBUG] AWS profile: ${aws_profile}"

    if [[ $# -lt 2 ]]; then
        echo "Too few arguments provided. Usage: ${usage}"
        return 1
    fi

    if [[ "${script_name}" == "k8s_utils" ]]; then
        if [[ -z "${PCC_PATH}" ]]; then
            echo "[ERROR] LOCAL sourcing enabled for '${script_name}', but PCC_PATH is not set"
            return 1
        fi
        echo "[DEBUG] Sourcing locally from ${PCC_PATH}/pingcloud-scripts/${script_name}/${script_name}.sh"
        source "${PCC_PATH}/pingcloud-scripts/${script_name}/${script_name}.sh"
        return 0
    fi

    local tmp_dir="/tmp/pingcloud-scripts/${version}"
    local src_bucket="pingcloud-scripts-dev"

    echo "[DEBUG] Temp directory: ${tmp_dir}"
    echo "[DEBUG] S3 bucket: ${src_bucket}"

    mkdir -p "${tmp_dir}"

    rm -f "${tmp_dir:?}/${script_name}.tar.gz" "${tmp_dir:?}/${script_name}.sh"

    if ! aws --no-cli-pager --profile "${aws_profile}" sts get-caller-identity > /dev/null 2>&1; then
        echo "pingcloud-scripts::source_script - Make sure you are logged into a current AWS session!"
        return 1
    fi

    echo "[DEBUG] Script not found locally. Fetching from S3"
    echo "[DEBUG] Downloading from s3://${src_bucket}/${script_name}/${version}/${script_name}.tar.gz"

    aws --profile "${aws_profile}" --only-show-errors s3 cp \
        "s3://${src_bucket}/${script_name}/${version}/${script_name}.tar.gz" \
        "${tmp_dir}/${script_name}.tar.gz"

    tar -xzf "${tmp_dir}/${script_name}.tar.gz" -C "${tmp_dir}"
    source "${tmp_dir}/${script_name}.sh"
}