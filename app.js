document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const loader = document.getElementById('loader');
    const tableHead = document.getElementById('tableHead');
    const tableBody = document.getElementById('tableBody');
    const noResults = document.getElementById('noResults');
    const rowCountDisplay = document.getElementById('rowCount');
    const paginationControls = document.getElementById('pagination');
    const columnToggleContainer = document.getElementById('columnToggle');

    // Filters
    const globalSearch = document.getElementById('globalSearch');
    const memberNameFilter = document.getElementById('memberNameFilter');
    const membershipNameFilter = document.getElementById('membershipNameFilter');
    const statusFilter = document.getElementById('statusFilter');
    
    // Export Buttons
    const exportCsvBtn = document.getElementById('exportCsv');
    const exportJsonBtn = document.getElementById('exportJson');

    // State
    let allData = [];
    let filteredData = [];
    const state = {
        currentPage: 1,
        rowsPerPage: 15,
        sortColumn: 'memberName',
        sortDirection: 'asc',
        columnVisibility: {}
    };

    // Column Configuration
    const columns = [
        { key: 'expand', label: '', width: '30px', visible: true, sortable: false, group: 'actions' },
        { key: 'memberName', label: 'Member Name', width: '150px', visible: true, sortable: true, group: 'member' },
        { key: 'membershipName', label: 'Membership Name', width: '200px', visible: true, sortable: true, group: 'membership' },
        { key: 'status', label: 'Status', width: '100px', visible: true, sortable: true, group: 'membership' },
        { key: 'sessionsAttended', label: 'Sessions', width: '80px', visible: true, sortable: true, group: 'sessions' },
        { key: 'classesLeft', label: 'Classes Left', width: '90px', visible: true, sortable: true, group: 'sessions' },
        { key: 'usageLimitForSessions', label: 'Session Limit', width: '100px', visible: true, sortable: true, group: 'sessions' },
        { key: 'freezeAttempts', label: 'Freeze Attempts', width: '120px', visible: true, sortable: true, group: 'freeze' },
        { key: 'frozenDays', label: 'Frozen Days', width: '100px', visible: true, sortable: true, group: 'freeze' },
        { key: 'freezeStartDate', label: 'Freeze Start', width: '110px', visible: true, sortable: true, group: 'freeze' },
        { key: 'freezeEndDate', label: 'Freeze End', width: '110px', visible: true, sortable: true, group: 'freeze' },
        { key: 'locationName', label: 'Location', width: '120px', visible: true, sortable: true, group: 'location' },
        { key: 'startDate', label: 'Start Date', width: '100px', visible: false, sortable: true, group: 'dates' },
        { key: 'endDate', label: 'End Date', width: '100px', visible: false, sortable: true, group: 'dates' },
        { key: 'createdAt', label: 'Created', width: '100px', visible: false, sortable: true, group: 'dates' },
        { key: 'paymentMethod', label: 'Payment Method', width: '120px', visible: false, sortable: true, group: 'payment' },
        { key: 'amountPaid', label: 'Amount Paid', width: '100px', visible: false, sortable: true, group: 'payment' },
        { key: 'moneyLeft', label: 'Money Left', width: '100px', visible: false, sortable: true, group: 'payment' },
        { key: 'paymentSource', label: 'Payment Source', width: '120px', visible: false, sortable: true, group: 'payment' },
        { key: 'membershipType', label: 'Membership Type', width: '130px', visible: false, sortable: true, group: 'membership' },
        { key: 'hostId', label: 'Host ID', width: '80px', visible: false, sortable: true, group: 'system' },
        { key: 'createdByUserName', label: 'Created By', width: '120px', visible: false, sortable: true, group: 'system' },
        { key: 'isFreezed', label: 'Currently Frozen', width: '110px', visible: false, sortable: true, group: 'freeze' },
        { key: 'isVoided', label: 'Voided', width: '70px', visible: false, sortable: true, group: 'system' }
    ];

    // View Mode Configurations
    const viewModes = {
        summary: {
            name: 'Summary View',
            description: 'Basic member and membership overview',
            columns: ['expand', 'memberName', 'membershipName', 'status', 'sessionsAttended', 'freezeAttempts', 'frozenDays', 'locationName']
        },
        detailed: {
            name: 'Detailed View', 
            description: 'Comprehensive member information',
            columns: ['expand', 'memberName', 'membershipName', 'status', 'sessionsAttended', 'classesLeft', 'usageLimitForSessions', 
                     'freezeAttempts', 'frozenDays', 'freezeStartDate', 'freezeEndDate', 'locationName', 'paymentMethod', 'amountPaid', 'membershipType']
        },
        'freeze-focused': {
            name: 'Freeze-Focused View',
            description: 'Freeze history and status details',
            columns: ['expand', 'memberName', 'membershipName', 'freezeAttempts', 'frozenDays', 'freezeStartDate', 'freezeEndDate', 
                     'isFreezed', 'status', 'locationName']
        }
    };

    // Initialize column visibility based on default view mode (summary)
    columns.forEach(col => {
        state.columnVisibility[col.key] = viewModes.summary.columns.includes(col.key);
    });

    // Expanded rows state
    const expandedRows = new Set();

    // --- INITIALIZATION ---
    async function initialize() {
        try {
            const response = await fetch('data.json');
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            allData = await response.json();
            
            if (!allData || !Array.isArray(allData) || allData.length === 0) {
                throw new Error('No valid data found in data.json');
            }
            
            setupColumns();
            populateFilters(allData);
            addEventListeners();
            render();
        } catch (error) {
            console.error("Failed to load or process data:", error);
            if (tableBody) {
                tableBody.innerHTML = `<tr><td colspan="10" class="error">Error: ${error.message}</td></tr>`;
            }
        } finally {
            if (loader) {
                loader.style.display = 'none';
            }
        }
    }

    // --- RENDERING PIPELINE ---
    function render() {
        // 1. Apply Filters
        applyFilters();

        // 2. Apply Sorting
        applySorting();
        
        // 3. Render UI Components
        renderTable();
        renderPagination();
        updateRowCount();
    }

    function applyFilters() {
        const globalSearchTerm = globalSearch.value.toLowerCase();
        const memberName = memberNameFilter.value;
        const membershipName = membershipNameFilter.value;
        const status = statusFilter.value;

        filteredData = allData.filter(item => {
            const matchesGlobalSearch = Object.entries(item).some(([key, val]) =>
                state.columnVisibility[key] && String(val).toLowerCase().includes(globalSearchTerm)
            );
            return matchesGlobalSearch &&
                   (!memberName || item.memberName === memberName) &&
                   (!membershipName || item.membershipName === membershipName) &&
                   (!status || item.status === status);
        });
        state.currentPage = 1; // Reset to first page after filtering
    }
    
    function applySorting() {
        filteredData.sort((a, b) => {
            const valA = a[state.sortColumn];
            const valB = b[state.sortColumn];

            if (valA < valB) return state.sortDirection === 'asc' ? -1 : 1;
            if (valA > valB) return state.sortDirection === 'asc' ? 1 : -1;
            return 0;
        });
    }

    // --- UI RENDERING FUNCTIONS ---
    function setupColumns() {
        // Clear existing headers
        tableHead.innerHTML = '';
        
        // Table Headers - only for visible columns
        const headerRow = document.createElement('tr');
        columns.forEach(col => {
            if (state.columnVisibility[col.key]) {
                const th = document.createElement('th');
                th.dataset.col = col.key;
                th.innerHTML = `${col.label} ${col.sortable ? '<i class="fa-solid fa-sort sort-icon"></i>' : ''}`;
                headerRow.appendChild(th);
            }
        });
        tableHead.appendChild(headerRow);

        // Column Toggle Checkboxes
        columnToggleContainer.innerHTML = columns.map(col => `
            <label>
                <input type="checkbox" class="column-toggle-cb" data-col="${col.key}" ${state.columnVisibility[col.key] ? 'checked' : ''}>
                ${col.label}
            </label>
        `).join('');
        
        updateColumnVisibility();
    }
    
    function renderTable() {
        tableBody.innerHTML = '';
        noResults.style.display = filteredData.length === 0 ? 'block' : 'none';
        
        const paginatedData = filteredData.slice(
            (state.currentPage - 1) * state.rowsPerPage,
            state.currentPage * state.rowsPerPage
        );

        paginatedData.forEach((item, index) => {
            const row = document.createElement('tr');
            const rowId = `row-${state.currentPage}-${index}`;
            row.dataset.rowId = rowId;
            
            columns.forEach(col => {
                if (state.columnVisibility[col.key]) {
                    const cell = document.createElement('td');
                    cell.dataset.col = col.key;
                    
                    let content = item[col.key] !== null && item[col.key] !== undefined ? item[col.key] : '-';
                    
                    if (col.key === 'expand') {
                        const isExpanded = expandedRows.has(rowId);
                        content = `<button class="expand-btn ${isExpanded ? 'expanded' : ''}" onclick="toggleRowExpansion('${rowId}')">
                            <i class="fa-solid fa-chevron-${isExpanded ? 'down' : 'right'}"></i>
                        </button>`;
                    } else if (col.key === 'status') {
                        content = `<span class="status-tag status-${content.replace(/\s+/g, '-')}">${content}</span>`;
                    }
                    
                    cell.innerHTML = content;
                    row.appendChild(cell);
                }
            });
            tableBody.appendChild(row);
            
            // Add drill-down row if expanded
            if (expandedRows.has(rowId)) {
                const visibleColumnCount = columns.filter(col => state.columnVisibility[col.key]).length;
                const detailRow = createDetailRow(item, visibleColumnCount);
                tableBody.appendChild(detailRow);
            }
        });

        updateColumnVisibility();
        updateSortIndicator();
    }
    
    function renderPagination() {
        const totalPages = Math.ceil(filteredData.length / state.rowsPerPage);
        paginationControls.innerHTML = '';

        if (totalPages <= 1) return;

        // Prev Button
        const prevButton = document.createElement('button');
        prevButton.innerHTML = '&laquo;';
        prevButton.disabled = state.currentPage === 1;
        prevButton.addEventListener('click', () => {
            state.currentPage--;
            render();
        });
        paginationControls.appendChild(prevButton);

        // Page Numbers (simplified)
        for (let i = 1; i <= totalPages; i++) {
            const pageButton = document.createElement('button');
            pageButton.innerText = i;
            if (i === state.currentPage) pageButton.classList.add('active');
            pageButton.addEventListener('click', () => {
                state.currentPage = i;
                render();
            });
            paginationControls.appendChild(pageButton);
        }

        // Next Button
        const nextButton = document.createElement('button');
        nextButton.innerHTML = '&raquo;';
        nextButton.disabled = state.currentPage === totalPages;
        nextButton.addEventListener('click', () => {
            state.currentPage++;
            render();
        });
        paginationControls.appendChild(nextButton);
    }

    function updateRowCount() {
        rowCountDisplay.textContent = `Showing ${filteredData.length > 0 ? (state.currentPage - 1) * state.rowsPerPage + 1 : 0} - ${Math.min(state.currentPage * state.rowsPerPage, filteredData.length)} of ${filteredData.length}`;
    }

    function updateSortIndicator() {
        tableHead.querySelectorAll('th').forEach(th => {
            th.classList.remove('sorted');
            const icon = th.querySelector('.sort-icon');
            icon.className = 'fa-solid fa-sort sort-icon';

            if (th.dataset.col === state.sortColumn) {
                th.classList.add('sorted');
                icon.className = `fa-solid fa-sort-${state.sortDirection === 'asc' ? 'up' : 'down'} sort-icon`;
            }
        });
    }
    
    function updateColumnVisibility() {
        columns.forEach(col => {
            const display = state.columnVisibility[col.key] ? '' : 'none';
            document.querySelectorAll(`[data-col="${col.key}"]`).forEach(el => el.style.display = display);
        });
    }

    // --- HELPER FUNCTIONS ---
    function populateFilters(data) {
        const populate = (key, selectElement) => {
            const uniqueValues = [...new Set(data.map(item => item[key]))].sort();
            uniqueValues.forEach(value => {
                const option = document.createElement('option');
                option.value = value;
                option.textContent = value;
                selectElement.appendChild(option);
            });
        };
        populate('memberName', memberNameFilter);
        populate('membershipName', membershipNameFilter);
        populate('status', statusFilter);
    }

    function exportToCsv(data) {
        const visibleColumns = columns.filter(c => state.columnVisibility[c.key]);
        const header = visibleColumns.map(c => c.title).join(',') + '\n';
        const rows = data.map(row => 
            visibleColumns.map(col => `"${String(row[col.key]).replace(/"/g, '""')}"`).join(',')
        ).join('\n');
        
        const blob = new Blob([header + rows], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = 'membership_data.csv';
        link.click();
    }

    // --- DRILL-DOWN FUNCTIONALITY ---
    function toggleRowExpansion(rowId) {
        if (expandedRows.has(rowId)) {
            expandedRows.delete(rowId);
        } else {
            expandedRows.add(rowId);
        }
        render(); // Re-render to update expansion state
    }

    function createDetailRow(item, colSpan) {
        const detailRow = document.createElement('tr');
        detailRow.classList.add('detail-row');
        
        const detailCell = document.createElement('td');
        detailCell.colSpan = colSpan;
        detailCell.classList.add('detail-content');
        
        // Parse freeze attempts from allFreezePairs
        const freezeDetails = item.allFreezePairs && item.allFreezePairs !== '' 
            ? item.allFreezePairs.split(' | ').map(pair => {
                const match = pair.match(/Attempt (\d+): (.+) to (.+)/);
                return match ? {
                    attempt: match[1],
                    startDate: match[2],
                    endDate: match[3]
                } : null;
            }).filter(Boolean)
            : [];

        detailCell.innerHTML = `
            <div class="detail-container">
                <div class="detail-section">
                    <h4><i class="fa-solid fa-snowflake"></i> Freeze History</h4>
                    ${freezeDetails.length > 0 ? `
                        <div class="freeze-attempts">
                            ${freezeDetails.map(freeze => `
                                <div class="freeze-attempt">
                                    <span class="attempt-number">Attempt ${freeze.attempt}</span>
                                    <span class="freeze-period">${freeze.startDate} → ${freeze.endDate}</span>
                                </div>
                            `).join('')}
                        </div>
                    ` : '<p class="no-data">No freeze attempts recorded</p>'}
                </div>
                
                <div class="detail-section">
                    <h4><i class="fa-solid fa-info-circle"></i> Additional Details</h4>
                    <div class="detail-grid">
                        <div class="detail-item">
                            <span class="detail-label">Payment Transaction ID:</span>
                            <span class="detail-value">${item.paymentTransactionId || '-'}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Sale Item ID:</span>
                            <span class="detail-value">${item.saleItemId || '-'}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Created By:</span>
                            <span class="detail-value">${item.createdByUserName || '-'}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Currently Frozen:</span>
                            <span class="detail-value ${item.isFreezed === 'true' ? 'frozen-yes' : 'frozen-no'}">
                                ${item.isFreezed === 'true' ? 'Yes' : 'No'}
                            </span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Voided:</span>
                            <span class="detail-value">${item.isVoided === 'true' ? 'Yes' : 'No'}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Host ID:</span>
                            <span class="detail-value">${item.hostId || '-'}</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        detailRow.appendChild(detailCell);
        return detailRow;
    }

    // Make toggleRowExpansion globally accessible
    window.toggleRowExpansion = toggleRowExpansion;

    // --- VIEW MODE FUNCTIONALITY ---
    function changeViewMode(mode) {
        if (!viewModes[mode]) return;
        
        const selectedMode = viewModes[mode];
        
        // Update column visibility based on view mode
        columns.forEach(col => {
            state.columnVisibility[col.key] = selectedMode.columns.includes(col.key);
        });
        
        // Clear expanded rows when changing view mode
        expandedRows.clear();
        
        // Update column toggle checkboxes
        setupColumns();
        
        // Re-render table
        render();
    }

    // --- EVENT LISTENERS ---
    function addEventListeners() {
        [globalSearch, memberNameFilter, membershipNameFilter, statusFilter].forEach(el => {
            el.addEventListener('input', render);
        });

        // View mode change handler
        const viewModeSelect = document.getElementById('viewMode');
        if (viewModeSelect) {
            viewModeSelect.addEventListener('change', (e) => {
                changeViewMode(e.target.value);
            });
        }

        tableHead.addEventListener('click', e => {
            const header = e.target.closest('th');
            if (!header) return;
            
            const colKey = header.dataset.col;
            const column = columns.find(col => col.key === colKey);
            if (!column || !column.sortable) return;
            
            if (state.sortColumn === colKey) {
                state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';
            } else {
                state.sortColumn = colKey;
                state.sortDirection = 'asc';
            }
            render();
        });

        columnToggleContainer.addEventListener('change', e => {
            const checkbox = e.target.closest('.column-toggle-cb');
            if (checkbox) {
                state.columnVisibility[checkbox.dataset.col] = checkbox.checked;
                updateColumnVisibility();
            }
        });

        exportCsvBtn.addEventListener('click', (e) => {
            e.preventDefault();
            exportToCsv(filteredData);
        });

        exportJsonBtn.addEventListener('click', (e) => {
            e.preventDefault();
            const jsonString = JSON.stringify(filteredData, null, 2);
            const blob = new Blob([jsonString], { type: 'application/json;charset=utf-8;' });
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = 'membership_data.json';
            link.click();
        });
    }

    // Start the application
    initialize();
});