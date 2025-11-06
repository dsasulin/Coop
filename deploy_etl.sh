#!/bin/bash
# ============================================================================
# Banking ETL Deployment Script
# Description: Deploy Spark jobs and Airflow DAGs to production
# Author: Data Engineering Team
# Date: 2025-01-06
# Version: 1.0
# ============================================================================

set -e  # Exit on error

# ============================================================================
# Configuration
# ============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Deployment paths (UPDATE THESE BASED ON YOUR ENVIRONMENT)
SPARK_JOBS_DEST="/opt/spark_jobs"
AIRFLOW_DAGS_DEST="/opt/airflow/dags"
BACKUP_DIR="/opt/backups/etl_$(date +%Y%m%d_%H%M%S)"

# Source paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_JOBS_SRC="${SCRIPT_DIR}/spark_jobs"
AIRFLOW_DAGS_SRC="${SCRIPT_DIR}/airflow_dags"

# Server configuration (for remote deployment)
# TODO: Update with actual server details
REMOTE_SERVER=""  # e.g., "user@cdp-server.company.com"
REMOTE_PORT="22"

# ============================================================================
# Helper Functions
# ============================================================================

print_header() {
    echo ""
    echo -e "${BLUE}============================================================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================================================${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# ============================================================================
# Pre-deployment Checks
# ============================================================================

pre_deployment_checks() {
    print_header "Running Pre-deployment Checks"

    # Check if source directories exist
    if [ ! -d "$SPARK_JOBS_SRC" ]; then
        print_error "Spark jobs directory not found: $SPARK_JOBS_SRC"
        exit 1
    fi
    print_success "Spark jobs directory found"

    if [ ! -d "$AIRFLOW_DAGS_SRC" ]; then
        print_error "Airflow DAGs directory not found: $AIRFLOW_DAGS_SRC"
        exit 1
    fi
    print_success "Airflow DAGs directory found"

    # Check Python syntax
    print_info "Checking Python syntax..."
    for file in "$SPARK_JOBS_SRC"/*.py; do
        python3 -m py_compile "$file"
        if [ $? -eq 0 ]; then
            print_success "  $(basename "$file") - syntax OK"
        else
            print_error "  $(basename "$file") - syntax error"
            exit 1
        fi
    done

    for file in "$AIRFLOW_DAGS_SRC"/*.py; do
        python3 -m py_compile "$file"
        if [ $? -eq 0 ]; then
            print_success "  $(basename "$file") - syntax OK"
        else
            print_error "  $(basename "$file") - syntax error"
            exit 1
        fi
    done

    print_success "All syntax checks passed"
}

# ============================================================================
# Backup Existing Files
# ============================================================================

backup_existing_files() {
    print_header "Backing Up Existing Files"

    mkdir -p "$BACKUP_DIR"

    # Backup Spark jobs
    if [ -d "$SPARK_JOBS_DEST" ]; then
        cp -r "$SPARK_JOBS_DEST" "$BACKUP_DIR/spark_jobs_backup"
        print_success "Backed up existing Spark jobs to $BACKUP_DIR/spark_jobs_backup"
    fi

    # Backup Airflow DAGs
    if [ -d "$AIRFLOW_DAGS_DEST" ]; then
        cp -r "$AIRFLOW_DAGS_DEST" "$BACKUP_DIR/airflow_dags_backup"
        print_success "Backed up existing Airflow DAGs to $BACKUP_DIR/airflow_dags_backup"
    fi
}

# ============================================================================
# Deploy Spark Jobs
# ============================================================================

deploy_spark_jobs() {
    print_header "Deploying Spark Jobs"

    # Create destination directory if it doesn't exist
    mkdir -p "$SPARK_JOBS_DEST"

    # Copy Spark jobs
    cp "$SPARK_JOBS_SRC"/*.py "$SPARK_JOBS_DEST/"
    chmod +x "$SPARK_JOBS_DEST"/*.py

    # List deployed files
    print_info "Deployed Spark jobs:"
    ls -lh "$SPARK_JOBS_DEST"/*.py

    print_success "Spark jobs deployed successfully"
}

# ============================================================================
# Deploy Airflow DAGs
# ============================================================================

deploy_airflow_dags() {
    print_header "Deploying Airflow DAGs"

    # Create destination directory if it doesn't exist
    mkdir -p "$AIRFLOW_DAGS_DEST"

    # Copy Airflow DAGs
    cp "$AIRFLOW_DAGS_SRC"/*.py "$AIRFLOW_DAGS_DEST/"
    chmod 644 "$AIRFLOW_DAGS_DEST"/*.py

    # List deployed files
    print_info "Deployed Airflow DAGs:"
    ls -lh "$AIRFLOW_DAGS_DEST"/*.py

    print_success "Airflow DAGs deployed successfully"
}

# ============================================================================
# Verify Deployment
# ============================================================================

verify_deployment() {
    print_header "Verifying Deployment"

    # Count files
    spark_jobs_count=$(ls -1 "$SPARK_JOBS_DEST"/*.py 2>/dev/null | wc -l)
    airflow_dags_count=$(ls -1 "$AIRFLOW_DAGS_DEST"/*.py 2>/dev/null | wc -l)

    print_info "Deployed files:"
    echo "  - Spark jobs: $spark_jobs_count"
    echo "  - Airflow DAGs: $airflow_dags_count"

    # Verify Airflow can see the DAGs (if airflow command is available)
    if command -v airflow &> /dev/null; then
        print_info "Verifying Airflow DAG visibility..."
        airflow dags list | grep banking_etl_pipeline
        if [ $? -eq 0 ]; then
            print_success "Airflow DAG 'banking_etl_pipeline' is visible"
        else
            print_warning "Airflow DAG 'banking_etl_pipeline' not found (may need scheduler refresh)"
        fi
    fi

    print_success "Deployment verification complete"
}

# ============================================================================
# Remote Deployment
# ============================================================================

deploy_remote() {
    print_header "Remote Deployment to $REMOTE_SERVER"

    if [ -z "$REMOTE_SERVER" ]; then
        print_error "REMOTE_SERVER not configured. Update the script with server details."
        exit 1
    fi

    # Create tarball
    TARBALL="/tmp/banking_etl_$(date +%Y%m%d_%H%M%S).tar.gz"
    tar -czf "$TARBALL" -C "$SCRIPT_DIR" spark_jobs airflow_dags
    print_success "Created deployment package: $TARBALL"

    # Copy to remote server
    scp -P "$REMOTE_PORT" "$TARBALL" "$REMOTE_SERVER:/tmp/"
    print_success "Copied package to remote server"

    # Extract and deploy on remote server
    ssh -p "$REMOTE_PORT" "$REMOTE_SERVER" << EOF
        set -e
        cd /tmp
        tar -xzf $(basename "$TARBALL")

        # Deploy Spark jobs
        mkdir -p "$SPARK_JOBS_DEST"
        cp spark_jobs/*.py "$SPARK_JOBS_DEST/"
        chmod +x "$SPARK_JOBS_DEST"/*.py

        # Deploy Airflow DAGs
        mkdir -p "$AIRFLOW_DAGS_DEST"
        cp airflow_dags/*.py "$AIRFLOW_DAGS_DEST/"
        chmod 644 "$AIRFLOW_DAGS_DEST"/*.py

        echo "Deployment complete on $(hostname)"
EOF

    print_success "Remote deployment completed"

    # Cleanup
    rm "$TARBALL"
}

# ============================================================================
# Rollback Function
# ============================================================================

rollback() {
    print_header "Rolling Back to Previous Version"

    if [ ! -d "$BACKUP_DIR" ]; then
        print_error "Backup directory not found: $BACKUP_DIR"
        print_error "Cannot rollback without backup"
        exit 1
    fi

    # Restore Spark jobs
    if [ -d "$BACKUP_DIR/spark_jobs_backup" ]; then
        cp -r "$BACKUP_DIR/spark_jobs_backup"/* "$SPARK_JOBS_DEST/"
        print_success "Restored Spark jobs from backup"
    fi

    # Restore Airflow DAGs
    if [ -d "$BACKUP_DIR/airflow_dags_backup" ]; then
        cp -r "$BACKUP_DIR/airflow_dags_backup"/* "$AIRFLOW_DAGS_DEST/"
        print_success "Restored Airflow DAGs from backup"
    fi

    print_success "Rollback completed"
}

# ============================================================================
# Main Deployment Function
# ============================================================================

deploy_local() {
    print_header "Starting Local Deployment"

    pre_deployment_checks
    backup_existing_files
    deploy_spark_jobs
    deploy_airflow_dags
    verify_deployment

    print_header "Deployment Summary"
    print_success "All components deployed successfully!"
    echo ""
    print_info "Next steps:"
    echo "  1. Verify Airflow UI: http://<airflow-host>:8080"
    echo "  2. Check DAG visibility: airflow dags list | grep banking"
    echo "  3. Test DAG: airflow dags test banking_etl_pipeline 2025-01-06"
    echo "  4. Trigger manually: airflow dags trigger banking_etl_pipeline"
    echo ""
    print_info "Backup location: $BACKUP_DIR"
    echo ""
}

# ============================================================================
# Usage Information
# ============================================================================

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy Banking ETL pipeline components (Spark jobs and Airflow DAGs)

OPTIONS:
    local       Deploy to local server (default)
    remote      Deploy to remote server
    rollback    Rollback to previous version
    help        Show this help message

EXAMPLES:
    # Local deployment
    $0 local

    # Remote deployment
    $0 remote

    # Rollback
    $0 rollback

CONFIGURATION:
    Edit the script to update these variables:
    - SPARK_JOBS_DEST: Destination for Spark jobs
    - AIRFLOW_DAGS_DEST: Destination for Airflow DAGs
    - REMOTE_SERVER: Remote server for deployment
    - REMOTE_PORT: SSH port for remote server

EOF
}

# ============================================================================
# Main Script Logic
# ============================================================================

main() {
    # Check if running with required permissions
    # (Uncomment if deployment requires sudo)
    # if [ "$EUID" -ne 0 ]; then
    #     print_error "Please run as root or with sudo"
    #     exit 1
    # fi

    # Parse command line arguments
    case "${1:-local}" in
        local)
            deploy_local
            ;;
        remote)
            deploy_remote
            ;;
        rollback)
            rollback
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
