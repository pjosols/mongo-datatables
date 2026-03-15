#!/bin/bash

# Development Loop for Mongo-DataTables

set -e

# Lock file to prevent multiple instances
LOCK_FILE="/tmp/mongo_datatables_loop.lock"
if [ -f "$LOCK_FILE" ]; then
    EXISTING_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [ -n "$EXISTING_PID" ] && kill -0 "$EXISTING_PID" 2>/dev/null; then
        echo "ERROR: Development loop already running (PID: $EXISTING_PID)"
        echo "If you're sure no loop is running, remove: $LOCK_FILE"
        exit 1
    else
        echo "Removing stale lock file..."
        rm -f "$LOCK_FILE"
    fi
fi

# Create lock file with current PID
echo $$ > "$LOCK_FILE"

# Cleanup function
cleanup() {
    rm -f "$LOCK_FILE"
    exit
}
trap cleanup EXIT INT TERM

# Configuration
NTFY_TOPIC="paulolsen-net"
MAX_ITERATIONS=${1:-10}  # Default to 10 iterations
PROJECTS_DIR="/Users/polsen/Projects"
LOG_FILE="$PROJECTS_DIR/balanced_development.log"

# Enhanced logging function
log_with_timestamp() {
    echo "$(date '+%Y-%m-%d %H:%M:%S'): $1" | tee -a "$LOG_FILE"
}

# Function to determine iteration type
get_iteration_type() {
    local iteration=$1
    local mod6=$((iteration % 6))
    
    case $mod6 in
        1|4) echo "EDITOR";;       # 33% - Editor protocol gaps
        2|5) echo "FEATURE";;      # 33% - Feature development
        3|0) echo "QUALITY";;      # 33% - Quality assurance
    esac
}

# Function to get iteration focus
get_iteration_focus() {
    local type=$1
    case $type in
        "EDITOR") echo "Close Editor protocol gaps per EDITOR_GAPS.md (priority order)";;
        "FEATURE") echo "Implement new DataTables feature with comprehensive testing";;
        "QUALITY") echo "Focus on code quality, performance, and testing improvements";;
    esac
}

# Function to get current credits
get_credits() {
    kiro-cli usage 2>/dev/null | grep -E "Credits.*remaining" | grep -oE "[0-9]+\.[0-9]+" | head -1 || echo "0.0"
}

# Function to send notification
send_notification() {
    local message="$1"
    local priority="${2:-default}"
    curl -s -d "$message" -H "Priority: $priority" -H "Title: mongo-datatables" "https://ntfy.sh/$NTFY_TOPIC" > /dev/null 2>&1 || true
}

# Function to create safety checkpoint
create_checkpoint() {
    local iteration=$1
    local type=$2
    log_with_timestamp "Creating safety checkpoint for iteration $iteration ($type)"
    cd mongo-datatables && git tag "pre-balanced-iteration-$iteration-$(date +%Y%m%d-%H%M%S)" 2>/dev/null || true
    cd "$PROJECTS_DIR"
}

# Function to capture performance baseline
capture_baseline() {
    log_with_timestamp "Capturing performance baseline..."
    cd flask-demo && source venv/bin/activate
    
    # Simple performance test
    local start_time=$(date +%s%N)
    curl -s -X POST http://localhost:5001/api/books -d '{"draw":1,"start":0,"length":100}' -H 'Content-Type: application/json' > /dev/null 2>&1 || true
    local end_time=$(date +%s%N)
    local duration=$(( (end_time - start_time) / 1000000 )) # Convert to milliseconds
    
    echo "BASELINE_PERFORMANCE_MS=$duration" >> "$LOG_FILE"
    cd "$PROJECTS_DIR"
}

# Ensure we're in the right directory
cd "$PROJECTS_DIR"

# Initial setup
log_with_timestamp "========================================="
log_with_timestamp "Starting Development Loop for mongo-datatables"
log_with_timestamp "Strategy: 33% Editor Protocol, 33% Feature Development, 33% Quality Assurance"
log_with_timestamp "Max iterations: $MAX_ITERATIONS"
log_with_timestamp "========================================="

INITIAL_CREDITS=$(get_credits)
send_notification "🚀 Loop Starting - $MAX_ITERATIONS iterations planned. Credits: $INITIAL_CREDITS"

# Main loop
for i in $(seq 1 $MAX_ITERATIONS); do
    ITERATION_TYPE=$(get_iteration_type $i)
    ITERATION_FOCUS=$(get_iteration_focus $ITERATION_TYPE)
    
    log_with_timestamp "========================================="
    log_with_timestamp "Starting iteration $i of $MAX_ITERATIONS"
    log_with_timestamp "Type: $ITERATION_TYPE"
    log_with_timestamp "Focus: $ITERATION_FOCUS"
    log_with_timestamp "========================================="
    
    # Create safety checkpoint
    create_checkpoint $i $ITERATION_TYPE
    
    # Capture baseline if needed
    if [ "$ITERATION_TYPE" = "QUALITY" ]; then
        capture_baseline
    fi
    
    ITERATION_START_CREDITS=$(get_credits)
    ITERATION_START_TIME=$(date +%s)
    
    # Create iteration-specific output file
    ITERATION_OUTPUT="/tmp/mongo_datatables_output_$i.txt"
    
    # Build context based on iteration type
    case $ITERATION_TYPE in
        "EDITOR")
            CONTEXT="EDITOR PROTOCOL ITERATION: Read EDITOR_GAPS.md in the project root for the prioritized list of gaps between our Editor implementation and the official DataTables Editor client/server protocol (https://editor.datatables.net/manual/server). Pick the highest-priority unfinished gap and implement it with comprehensive tests. Work only in editor.py and its tests — do not modify datatables.py or query_builder.py. After implementation, update EDITOR_GAPS.md to mark the item as done."
            ;;
        "FEATURE")
            CONTEXT="FEATURE DEVELOPMENT ITERATION: Research and implement a new high-value DataTables feature. Focus on comprehensive testing and documentation. Ensure backward compatibility and integration with existing features."
            ;;
        "QUALITY")
            CONTEXT="QUALITY ASSURANCE ITERATION: Focus on code quality, performance optimization, test coverage expansion, and technical debt reduction. No new features - improve what exists."
            ;;
    esac
    
    log_with_timestamp "Executing development orchestrator for iteration $i ($ITERATION_TYPE)..."
    
    # Run the orchestrator with type-specific context
    kiro-cli chat --agent mongo_datatables_orchestrator --no-interactive --trust-all-tools \
        "Execute the development workflow. Current iteration: $i of $MAX_ITERATIONS. Working in: $PROJECTS_DIR.

ITERATION TYPE: $ITERATION_TYPE
FOCUS: $ITERATION_FOCUS

CURRENT STATE:
- mongo-datatables library with comprehensive feature set
- All systems working correctly 
- Extensive test coverage with passing test suite

$CONTEXT

QUALITY GATES (MANDATORY):
- All existing tests must pass
- No performance regressions (benchmark against baseline)
- API compatibility maintained (backward compatible)
- Real-world validation (Flask + Django demos work)
- Documentation updated for any changes

ENHANCED REQUIREMENTS:
1. COMPREHENSIVE TESTING: Create tests before implementation
2. PERFORMANCE AWARENESS: Monitor and optimize performance
3. BACKWARD COMPATIBILITY: Never break existing functionality
4. DOCUMENTATION: Update examples and API docs
5. QUALITY METRICS: Improve or maintain code quality scores

REALISTIC AGENT CAPABILITIES:
- Analyze code, implement features, run tests
- Test API endpoints with curl (not browser UI)
- Create comprehensive test suites
- Update documentation and examples
- Performance benchmarking via API calls

WORKFLOW FOR $ITERATION_TYPE:
1. Research and analyze opportunities for this iteration type
2. Implement changes with comprehensive testing
3. Validate all functionality and performance
4. Update documentation as needed

Focus on sustainable development that balances innovation with reliability." \
        > "$ITERATION_OUTPUT" 2>&1 &
    
    ORCHESTRATOR_PID=$!
    log_with_timestamp "AGENT[orchestrator]: STARTED - PID: $ORCHESTRATOR_PID, Type: $ITERATION_TYPE"
    
    # Monitor orchestrator progress with timeout
    MONITOR_COUNT=0
    MAX_MONITOR_CYCLES=30  # 15 minutes max
    while kill -0 $ORCHESTRATOR_PID 2>/dev/null && [ $MONITOR_COUNT -lt $MAX_MONITOR_CYCLES ]; do
        sleep 30
        MONITOR_COUNT=$((MONITOR_COUNT + 1))
        
        case $MONITOR_COUNT in
            2) log_with_timestamp "AGENT[orchestrator]: PROGRESS - 1 minute elapsed ($ITERATION_TYPE)";;
            4) log_with_timestamp "AGENT[orchestrator]: PROGRESS - 2 minutes elapsed ($ITERATION_TYPE)";;
            8) log_with_timestamp "AGENT[orchestrator]: PROGRESS - 4 minutes elapsed ($ITERATION_TYPE)";;
            12) log_with_timestamp "AGENT[orchestrator]: WARNING - 6 minutes elapsed ($ITERATION_TYPE)";;
            16) log_with_timestamp "AGENT[orchestrator]: TIMEOUT_WARNING - 8 minutes elapsed ($ITERATION_TYPE)";;
        esac
        
        # Check for completion
        if [ -f "$ITERATION_OUTPUT" ] && grep -q "COMPLETE\|SUCCESS\|FINISHED" "$ITERATION_OUTPUT" 2>/dev/null; then
            log_with_timestamp "AGENT[orchestrator]: COMPLETING - Success signal detected ($ITERATION_TYPE)"
            break
        fi
    done
    
    # Handle timeout
    if [ $MONITOR_COUNT -ge $MAX_MONITOR_CYCLES ] && kill -0 $ORCHESTRATOR_PID 2>/dev/null; then
        log_with_timestamp "AGENT[orchestrator]: TIMEOUT - Killing stuck process after 15 minutes"
        kill $ORCHESTRATOR_PID 2>/dev/null || true
        send_notification "⚠️ Iteration $i Timeout - $ITERATION_TYPE iteration exceeded 10 minutes and was terminated"
    fi
    
    # Wait for orchestrator to complete
    wait $ORCHESTRATOR_PID 2>/dev/null || true
    
    # Process output for logging
    if [ -f "$ITERATION_OUTPUT" ]; then
        while IFS= read -r line; do
            echo "$(date '+%H:%M:%S') ORCHESTRATOR: $line" >> "$LOG_FILE"
        done < "$ITERATION_OUTPUT"
        rm -f "$ITERATION_OUTPUT"  # Clean up temp file
    fi
    
    ITERATION_END_TIME=$(date +%s)
    ITERATION_END_CREDITS=$(get_credits)
    ITERATION_DURATION=$((ITERATION_END_TIME - ITERATION_START_TIME))
    ITERATION_CREDITS_USED=$(echo "$ITERATION_START_CREDITS - $ITERATION_END_CREDITS" | bc -l 2>/dev/null || echo "0.0")
    
    # Format duration
    DURATION_MIN=$((ITERATION_DURATION / 60))
    DURATION_SEC=$((ITERATION_DURATION % 60))
    
    log_with_timestamp "Development iteration $i ($ITERATION_TYPE) completed"
    log_with_timestamp "Duration: ${DURATION_MIN}m ${DURATION_SEC}s, Credits used: $ITERATION_CREDITS_USED"
    
    # Send progress notification
    send_notification "✅ Iteration $i Complete ($ITERATION_TYPE) - Duration: ${DURATION_MIN}m ${DURATION_SEC}s, Credits: $ITERATION_CREDITS_USED"
done

# Final summary
FINAL_CREDITS=$(get_credits)
TOTAL_CREDITS_USED=$(echo "$INITIAL_CREDITS - $FINAL_CREDITS" | bc -l 2>/dev/null || echo "0.0")

log_with_timestamp "========================================="
log_with_timestamp "Development loop completed after $MAX_ITERATIONS iterations"
log_with_timestamp "Total credits used: $TOTAL_CREDITS_USED"
log_with_timestamp "========================================="

# Final notification
send_notification "🏁 Loop Complete - $MAX_ITERATIONS iterations finished. Total credits: $TOTAL_CREDITS_USED." "high"

log_with_timestamp "Development loop completed. Check detailed logs above for full results."
