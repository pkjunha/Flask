# This code implements a web server using Flask.
# It runs in a Python 3 environment.
# You must first install the libraries with: pip install Flask google-api-python-client google-auth
# The time module is built into Python.

from flask import Flask, jsonify, render_template_string, request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import re
import time
import random
import os

app = Flask(__name__)

SERVICE_ACCOUNT_FILE = 'credentials.json'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, SERVICE_ACCOUNT_FILE)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

try:
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
except FileNotFoundError:
    print("WARNING: 'credentials.json' file not found. API functionality will be disabled.")
    creds = None

def get_spreadsheet_id_from_url(url):
    match = re.search(r'spreadsheets/d/([a-zA-Z0-9-_]+)', url)
    if match:
        return match.group(1)
    return None

SAVED_STATES = {}

# --- Exponential Backoff Retry Logic ---
def with_retry(api_call, max_retries=5, initial_delay=1):
    """
    Applies exponential backoff retry logic to an API call.
    """
    for retry_count in range(max_retries):
        try:
            return api_call()
        except HttpError as e:
            if e.resp.status in [429, 500, 503]:
                delay = initial_delay * (2 ** retry_count) + random.uniform(0, 1)
                print(f"API error occurred ({e.resp.status}), retrying {retry_count + 1}/{max_retries}. Waiting {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                raise
        except Exception as e:
            delay = initial_delay * (2 ** retry_count) + random.uniform(0, 1)
            print(f"Unknown error occurred ({e}). Retrying {retry_count + 1}/{max_retries}. Waiting {delay:.2f} seconds...")
            time.sleep(delay)
    raise Exception("All retry attempts failed.")

@app.route('/')
def index():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Spreadsheet Column Reorder</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body { font-family: 'Arial', sans-serif; }
            .draggable-item { cursor: grab; }
            .draggable-item:active { cursor: grabbing; }
            .hidden { display: none; }
            .drag-over-top {
                border-top: 3px solid #2563eb; /* Blue-600 */
                margin-top: -3px;
            }
            .drag-over-bottom {
                border-bottom: 3px solid #2563eb; /* Blue-600 */
                margin-bottom: -3px;
            }
        </style>
    </head>
    <body class="bg-gray-100 min-h-screen p-8">
        <div class="max-w-4xl mx-auto bg-white p-8 rounded-2xl shadow-xl">
            <h1 class="text-3xl font-extrabold text-center text-gray-800 mb-6">
                Spreadsheet Column Reorder Simulator
            </h1>
            <p class="text-center text-gray-500 mb-8">
                Enter a Google Spreadsheet link and press 'Load'.
            </p>

            <!-- Spreadsheet link input form -->
            <div class="flex flex-col md:flex-row gap-4 mb-8">
                <input type="text" id="spreadsheet-url" class="flex-grow px-4 py-2 border rounded-full shadow-inner focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder="Paste your spreadsheet URL here">
                <button id="load-button" class="px-6 py-3 bg-indigo-500 text-white font-semibold rounded-full shadow-lg hover:bg-indigo-600 transition-colors duration-300 transform active:scale-95">
                    Load
                </button>
            </div>
            
            <p id="loading-message" class="hidden text-center text-indigo-500 mb-4">
                Loading data...
            </p>
            <p id="error-message" class="hidden text-center text-red-500 mb-4">
                Failed to load data. Please check the URL or permissions.
            </p>
            
            <!-- Sheet selection dropdown -->
            <div id="sheet-selector-container" class="mb-8 p-6 bg-gray-50 rounded-xl border border-gray-200 hidden">
                <h2 class="text-xl font-bold text-gray-700 mb-4">Select a Sheet</h2>
                <select id="sheet-name" class="w-full p-2 border rounded-md shadow-inner focus:outline-none focus:ring-2 focus:ring-blue-500">
                    <option value="">Select a sheet</option>
                </select>
                <button id="load-sheet-button" class="mt-4 px-6 py-3 bg-blue-500 text-white font-semibold rounded-full shadow-lg hover:bg-blue-600 transition-colors duration-300 transform hover:scale-105 active:scale-95 w-full">
                    Load Selected Sheet
                </button>
            </div>

            <!-- Available columns container -->
            <div id="available-columns-container" class="mb-8 p-6 bg-gray-50 rounded-xl border border-gray-200 hidden">
                <h2 class="text-xl font-bold text-gray-700 mb-4">Available Columns</h2>
                <div id="available-columns" class="flex flex-wrap gap-4 justify-center">
                    <p class="text-gray-400 font-medium">Enter a URL to load columns.</p>
                </div>
            </div>

            <!-- Selected columns container -->
            <div id="selected-columns-container" class="mb-8 p-6 bg-green-50 rounded-xl border border-green-200">
                <h2 class="text-xl font-bold text-gray-700 mb-4 flex items-center gap-2">
                    Selected Columns
                    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="text-green-500">
                        <path d="M12 5l7 7-7 7" />
                        <path d="M5 12h14" />
                    </svg>
                </h2>
                <div id="selected-columns" class="flex flex-col space-y-4">
                    <p id="selected-placeholder" class="text-gray-400 font-medium text-center">No columns selected.</p>
                </div>
            </div>

            <!-- Reset, Revert and Save buttons -->
            <div class="flex flex-col md:flex-row justify-center items-center gap-4">
                <!-- Checkbox to keep only selected columns -->
                <div class="flex items-center gap-2">
                    <input type="checkbox" id="keep-selected-columns-only-checkbox" class="w-4 h-4 text-green-600 bg-gray-100 border-gray-300 rounded focus:ring-green-500">
                    <label for="keep-selected-columns-only-checkbox" class="text-gray-700 font-semibold text-sm select-none">Keep only selected columns</label>
                </div>
                <button id="reset-button" class="flex items-center gap-2 px-6 py-3 bg-red-500 text-white font-semibold rounded-full shadow-lg hover:bg-red-600 transition-colors duration-300 transform hover:scale-105 active:scale-95">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M3 6h18" />
                        <path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6" />
                        <path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2" />
                        <line x1="10" x2="10" y1="11" y2="17" />
                        <line x1="14" x2="14" y1="11" y2="17" />
                    </svg>
                    Reset Selection
                </button>
                <button id="revert-button" class="flex items-center gap-2 px-6 py-3 bg-yellow-500 text-white font-semibold rounded-full shadow-lg hover:bg-yellow-600 transition-colors duration-300 transform hover:scale-105 active:scale-95 disabled:bg-gray-400 disabled:cursor-not-allowed" disabled>
                    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M11 5L4 12l7 7" />
                        <path d="M4 12h16" />
                    </svg>
                    Revert
                </button>
                <button id="save-button" class="flex items-center gap-2 px-6 py-3 bg-green-500 text-white font-semibold rounded-full shadow-lg hover:bg-green-600 transition-colors duration-300 transform hover:scale-105 active:scale-95">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z" />
                        <polyline points="17 21 17 13 7 13 7 21" />
                        <polyline points="7 3 7 8 15 8" />
                    </svg>
                    Save Column Order
                </button>
            </div>
            <p id="save-message" class="hidden text-center text-green-500 mt-4">
                Column order successfully saved!
            </p>
            <p id="save-error-message" class="hidden text-center text-red-500 mt-4">
                An error occurred while saving.
            </p>
            <p id="revert-message" class="hidden text-center text-yellow-500 mt-4">
                Revert successful!
            </p>
            <p id="revert-error-message" class="hidden text-center text-red-500 mt-4">
                An error occurred while reverting.
            </p>
        </div>

        <script>
            // Execute the script after all DOM elements are loaded.
            document.addEventListener('DOMContentLoaded', () => {
                const availableColumnsContainer = document.getElementById('available-columns-container');
                const availableColumnsDiv = document.getElementById('available-columns');
                const selectedColumnsDiv = document.getElementById('selected-columns');
                const selectedPlaceholder = document.getElementById('selected-placeholder');
                const resetButton = document.getElementById('reset-button');
                const saveButton = document.getElementById('save-button');
                const revertButton = document.getElementById('revert-button');
                const loadButton = document.getElementById('load-button');
                const urlInput = document.getElementById('spreadsheet-url');
                const sheetSelectorContainer = document.getElementById('sheet-selector-container');
                const sheetNameSelect = document.getElementById('sheet-name');
                const loadSheetButton = document.getElementById('load-sheet-button');
                const loadingMessage = document.getElementById('loading-message');
                const errorMessage = document.getElementById('error-message');
                const saveMessage = document.getElementById('save-message');
                const saveErrorMessage = document.getElementById('save-error-message');
                const revertMessage = document.getElementById('revert-message');
                const revertErrorMessage = document.getElementById('revert-error-message');
                const keepSelectedColumnsOnlyCheckbox = document.getElementById('keep-selected-columns-only-checkbox');

                let columnsData = [];
                let draggedElement = null;
                let currentSpreadsheetId = null;

                // Event to load the sheet list when the URL is entered.
                loadButton.addEventListener('click', async () => {
                    const url = urlInput.value;
                    if (!url) {
                        errorMessage.textContent = 'Please enter a URL.';
                        errorMessage.classList.remove('hidden');
                        return;
                    }

                    console.log('Load button clicked: Starting API call');
                    loadingMessage.textContent = 'Loading sheet list...';
                    loadingMessage.classList.remove('hidden');
                    errorMessage.classList.add('hidden');
                    sheetSelectorContainer.classList.add('hidden');
                    
                    try {
                        const response = await fetch('/api/sheets', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ url: url })
                        });

                        const data = await response.json();
                        if (response.ok) {
                            console.log('API call successful, sheet list:', data.sheets);
                            currentSpreadsheetId = data.spreadsheet_id;
                            populateSheetSelect(data.sheets);
                            sheetSelectorContainer.classList.remove('hidden');
                        } else {
                            console.error('API call failed:', data);
                            errorMessage.textContent = data.error || 'Failed to load sheet list. (Response error)';
                            errorMessage.classList.remove('hidden');
                        }
                    } catch (error) {
                        console.error('Network error:', error);
                        errorMessage.textContent = 'A network error occurred. Please try again later.';
                        errorMessage.classList.remove('hidden');
                    } finally {
                        loadingMessage.classList.add('hidden');
                    }
                });

                // Function to load columns for the selected sheet and update the UI.
                const loadAndRenderColumns = async () => {
                    const sheetName = sheetNameSelect.value;
                    if (!sheetName) {
                        errorMessage.textContent = 'Please select a sheet.';
                        errorMessage.classList.remove('hidden');
                        return;
                    }
                    
                    // Reset UI state
                    columnsData = [];
                    renderAvailableColumns();
                    renderSelectedColumns();
                    availableColumnsContainer.classList.add('hidden');

                    loadingMessage.textContent = 'Loading column data...';
                    loadingMessage.classList.remove('hidden');
                    errorMessage.classList.add('hidden');
                    
                    try {
                        const response = await fetch('/api/columns', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ spreadsheet_id: currentSpreadsheetId, sheet_name: sheetName })
                        });

                        const data = await response.json();
                        if (response.ok) {
                            console.log('Column data loaded successfully:', data);
                            columnsData = data.map(name => ({ name, selected: false }));
                            renderAvailableColumns();
                            renderSelectedColumns();
                            saveButton.classList.remove('hidden');
                            availableColumnsContainer.classList.remove('hidden');
                        } else {
                            console.error('Column data API failed:', data);
                            errorMessage.textContent = data.error || 'Failed to load column data.';
                            errorMessage.classList.remove('hidden');
                        }
                    } catch (error) {
                        console.error('Column data network error:', error);
                        errorMessage.textContent = 'A network error occurred. Please try again later.';
                        errorMessage.classList.remove('hidden');
                    } finally {
                        loadingMessage.classList.add('hidden');
                    }
                };

                // 'Load Selected Sheet' button event.
                loadSheetButton.addEventListener('click', loadAndRenderColumns);
                
                // Save column order button event.
                saveButton.addEventListener('click', async () => {
                    const selectedColumns = columnsData.filter(col => col.selected).map(col => col.name);
                    const sheetName = sheetNameSelect.value;
                    const keepSelectedOnly = keepSelectedColumnsOnlyCheckbox.checked;
                    
                    if (selectedColumns.length === 0) {
                        saveErrorMessage.textContent = 'No columns to save. Please select columns first.';
                        saveErrorMessage.classList.remove('hidden');
                        return;
                    }
                    
                    loadingMessage.textContent = 'Saving column order...';
                    loadingMessage.classList.remove('hidden');
                    saveErrorMessage.classList.add('hidden');
                    saveMessage.classList.add('hidden');
                    revertMessage.classList.add('hidden');
                    revertErrorMessage.classList.add('hidden');

                    try {
                        const response = await fetch('/api/save', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ 
                                spreadsheet_id: currentSpreadsheetId,
                                sheet_name: sheetName,
                                new_order: selectedColumns,
                                keep_selected_only: keepSelectedOnly
                            })
                        });

                        const data = await response.json();
                        if (response.ok) {
                            console.log('Save successful:', data);
                            saveMessage.classList.remove('hidden');
                            revertButton.disabled = !keepSelectedOnly; // Enable revert only after a deletion save
                            setTimeout(() => saveMessage.classList.add('hidden'), 2000);
                            await loadAndRenderColumns();

                        } else {
                            console.error('Save API failed:', data);
                            saveErrorMessage.textContent = data.error || 'An error occurred while saving.';
                            saveErrorMessage.classList.remove('hidden');
                        }
                    } catch (error) {
                        console.error('Save network error:', error);
                        saveErrorMessage.textContent = 'A network error occurred. Please try again later.';
                        saveErrorMessage.classList.remove('hidden');
                    } finally {
                        loadingMessage.classList.add('hidden');
                    }
                });

                // Revert button event.
                revertButton.addEventListener('click', async () => {
                    const sheetName = sheetNameSelect.value;
                    
                    loadingMessage.textContent = 'Reverting changes...';
                    loadingMessage.classList.remove('hidden');
                    saveMessage.classList.add('hidden');
                    saveErrorMessage.classList.add('hidden');
                    revertMessage.classList.add('hidden');
                    revertErrorMessage.classList.add('hidden');

                    try {
                        const response = await fetch('/api/revert', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                spreadsheet_id: currentSpreadsheetId,
                                sheet_name: sheetName
                            })
                        });
                        
                        const data = await response.json();
                        if (response.ok) {
                            revertMessage.classList.remove('hidden');
                            setTimeout(() => revertMessage.classList.add('hidden'), 2000);
                            revertButton.disabled = true;
                            await loadAndRenderColumns();
                        } else {
                            revertErrorMessage.textContent = data.error || 'An error occurred while reverting.';
                            revertErrorMessage.classList.remove('hidden');
                        }
                    } catch (error) {
                        revertErrorMessage.textContent = 'A network error occurred during revert.';
                        revertErrorMessage.classList.remove('hidden');
                    } finally {
                        loadingMessage.classList.add('hidden');
                    }
                });

                // Function to populate the sheet list dropdown.
                const populateSheetSelect = (sheets) => {
                    sheetNameSelect.innerHTML = '<option value="">Select a sheet</option>';
                    if (sheets.length === 0) {
                        const option = document.createElement('option');
                        option.textContent = 'No sheets found.';
                        sheetNameSelect.appendChild(option);
                        sheetNameSelect.disabled = true;
                    } else {
                        sheets.forEach(sheet => {
                            const option = document.createElement('option');
                            option.value = sheet.title;
                            option.textContent = sheet.title;
                            sheetNameSelect.appendChild(option);
                        });
                        sheetNameSelect.disabled = false;
                    }
                };

                // Function to render available columns on the screen.
                const renderAvailableColumns = () => {
                    availableColumnsDiv.innerHTML = '';
                    const available = columnsData.filter(col => !col.selected);
                    if (available.length === 0) {
                        availableColumnsDiv.innerHTML = '<p class="text-gray-400 font-medium">All columns have been selected!</p>';
                    } else {
                        available.forEach(col => {
                            const button = document.createElement('button');
                            button.className = 'px-6 py-3 bg-blue-500 text-white font-semibold rounded-full shadow-lg hover:bg-blue-600 transition-colors duration-300 transform hover:scale-105 active:scale-95';
                            button.textContent = col.name;
                            button.addEventListener('click', () => {
                                selectColumn(col.name);
                            });
                            availableColumnsDiv.appendChild(button);
                        });
                    }
                };
                
                // Function to add a column to the selected list.
                const selectColumn = (name) => {
                    // This is the updated logic to maintain the correct order
                    const columnToSelect = columnsData.find(col => col.name === name);
                    if (columnToSelect) {
                        // Mark the column as selected
                        columnToSelect.selected = true;
                        
                        // Separate selected and unselected columns to rebuild the array
                        const selected = columnsData.filter(col => col.selected);
                        const unselected = columnsData.filter(col => !col.selected);
                        
                        // Reconstruct the columnsData array with selected items first, in order of selection.
                        columnsData = [...selected, ...unselected];
                        
                        renderAvailableColumns();
                        renderSelectedColumns();
                    }
                };

                // Function to unselect a column and move it back to available.
                const unselectColumn = (name) => {
                    const unselectedColumnIndex = columnsData.findIndex(col => col.name === name);
                    if (unselectedColumnIndex !== -1) {
                        columnsData[unselectedColumnIndex].selected = false;
                        renderAvailableColumns();
                        renderSelectedColumns();
                    }
                };

                // Function to render selected columns on the screen with drag-and-drop.
                const renderSelectedColumns = () => {
                    selectedColumnsDiv.innerHTML = '';
                    const selected = columnsData.filter(col => col.selected);
                    
                    if (selected.length === 0) {
                        selectedPlaceholder.classList.remove('hidden');
                    } else {
                        selectedPlaceholder.classList.add('hidden');
                        selected.forEach((col, index) => {
                            const div = document.createElement('div');
                            div.className = 'relative flex items-center gap-4 bg-green-100 p-4 rounded-xl shadow-md draggable-item transition-opacity duration-200 group';
                            div.textContent = col.name;
                            div.setAttribute('draggable', true);
                            div.dataset.name = col.name;
                            
                            // Add order number
                            const orderSpan = document.createElement('span');
                            orderSpan.className = 'text-lg font-bold text-green-700';
                            orderSpan.textContent = (index + 1) + '.';
                            div.prepend(orderSpan);

                            // Add 'X' button
                            const removeBtn = document.createElement('span');
                            removeBtn.textContent = 'X';
                            removeBtn.className = 'absolute right-4 text-gray-500 hover:text-red-500 cursor-pointer text-xl font-bold transition-colors duration-200 opacity-0 group-hover:opacity-100';
                            removeBtn.addEventListener('click', (e) => {
                                e.stopPropagation(); // Prevent drag event
                                unselectColumn(col.name);
                            });
                            div.appendChild(removeBtn);

                            selectedColumnsDiv.appendChild(div);
                        });
                    }
                };

                // Optimized drag-and-drop event listeners
                selectedColumnsDiv.addEventListener('dragstart', (e) => {
                    draggedElement = e.target.closest('.draggable-item');
                    if (draggedElement) {
                        e.dataTransfer.effectAllowed = 'move';
                        setTimeout(() => {
                            draggedElement.classList.add('opacity-50');
                        }, 0);
                    }
                });

                selectedColumnsDiv.addEventListener('dragover', (e) => {
                    e.preventDefault();
                    const targetElement = e.target.closest('.draggable-item');
                    if (targetElement && targetElement !== draggedElement) {
                        const rect = targetElement.getBoundingClientRect();
                        const y = e.clientY - rect.top;

                        // Clear all drag-over classes
                        Array.from(selectedColumnsDiv.children).forEach(el => {
                            el.classList.remove('drag-over-top', 'drag-over-bottom');
                        });

                        // Add drag-over class based on drag direction
                        if (y < rect.height / 2) {
                            targetElement.classList.add('drag-over-top');
                        } else {
                            targetElement.classList.add('drag-over-bottom');
                        }
                    }
                });

                selectedColumnsDiv.addEventListener('dragleave', (e) => {
                    const targetElement = e.target.closest('.draggable-item');
                    if (targetElement) {
                        targetElement.classList.remove('drag-over-top', 'drag-over-bottom');
                    }
                });

                selectedColumnsDiv.addEventListener('drop', (e) => {
                    e.preventDefault();
                    const targetElement = e.target.closest('.draggable-item');

                    // Clear all drag-over classes on drop
                    Array.from(selectedColumnsDiv.children).forEach(el => {
                        el.classList.remove('drag-over-top', 'drag-over-bottom');
                    });

                    if (draggedElement && targetElement && draggedElement !== targetElement) {
                        const fromIndex = Array.from(selectedColumnsDiv.children).indexOf(draggedElement);
                        const toIndex = Array.from(selectedColumnsDiv.children).indexOf(targetElement);

                        const draggedName = draggedElement.dataset.name;
                        const targetName = targetElement.dataset.name;

                        // Find the data items
                        const draggedColIndex = columnsData.findIndex(col => col.name === draggedName);
                        const targetColIndex = columnsData.findIndex(col => col.name === targetName);
                        
                        // Extract the selected items for reordering
                        const selectedItems = columnsData.filter(col => col.selected);
                        const unselectedItems = columnsData.filter(col => !col.selected);

                        // Remove the dragged item from its current position
                        const [removed] = selectedItems.splice(fromIndex, 1);
                        
                        // Insert it at the new position
                        selectedItems.splice(toIndex, 0, removed);

                        // Update the main columnsData array
                        columnsData = [...selectedItems, ...unselectedItems];

                        // Re-render to reflect the new order and update numbers
                        renderSelectedColumns();
                    }
                    if (draggedElement) {
                        draggedElement.classList.remove('opacity-50');
                        draggedElement = null;
                    }
                });

                // Reset selection function.
                resetButton.addEventListener('click', () => {
                    columnsData.forEach(col => col.selected = false);
                    renderAvailableColumns();
                    renderSelectedColumns();
                });
            });
        </script>
    </body>
    </html>
    """
    return render_template_string(html_content)

# API endpoint: returns a list of sheets.
@app.route('/api/sheets', methods=['POST'])
def get_sheets():
    if not creds:
        return jsonify({'error': 'No authentication credentials.'}), 500
    
    data = request.json
    url = data.get('url')
    
    if not url:
        return jsonify({'error': 'URL not provided.'}), 400
    
    spreadsheet_id = get_spreadsheet_id_from_url(url)
    if not spreadsheet_id:
        return jsonify({'error': 'Invalid spreadsheet URL. Please ensure you entered a correct URL.'}), 400
    
    try:
        service = build('sheets', 'v4', credentials=creds)
        
        sheet_metadata = with_retry(lambda: service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute())
        
        sheets = sheet_metadata.get('sheets', [])
        sheet_titles = [{'title': sheet.get('properties', {}).get('title')} for sheet in sheets]

        return jsonify({'sheets': sheet_titles, 'spreadsheet_id': spreadsheet_id})
    except HttpError as e:
        error_details = e.content.decode('utf-8')
        if 'Requested entity was not found' in error_details:
             return jsonify({'error': 'Invalid spreadsheet ID or not found.'}), 404
        elif 'The caller does not have permission' in error_details:
             return jsonify({'error': 'Please ensure the service account email has edit permission for the spreadsheet.'}), 403
        else:
             return jsonify({'error': f'An error occurred during the API call: {e}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# API endpoint: returns column data.
@app.route('/api/columns', methods=['POST'])
def get_columns():
    if not creds:
        return jsonify({'error': 'No authentication credentials.'}), 500
    
    data = request.json
    spreadsheet_id = data.get('spreadsheet_id')
    sheet_name = data.get('sheet_name')
    
    if not spreadsheet_id or not sheet_name:
        return jsonify({'error': 'Spreadsheet ID or sheet name not provided.'}), 400
    
    try:
        service = build('sheets', 'v4', credentials=creds)
        
        # Add quotes to the sheet name if they are missing.
        range_name = f"'{sheet_name}'!1:1" if "'" not in sheet_name else f"{sheet_name}!1:1"
        
        # Wrap the API call with the with_retry function.
        result = with_retry(lambda: service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute())
        
        values = result.get('values', [[]])
        
        if not values or not values[0]:
            return jsonify({'error': f"Sheet '{sheet_name}' has no data, or the sheet name is incorrect."}), 404

        initial_columns = values[0]
        return jsonify(initial_columns)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# API endpoint: saves the column order.
@app.route('/api/save', methods=['POST'])
def save_columns():
    if not creds:
        return jsonify({'error': 'No authentication credentials.'}), 500
    
    data = request.json
    spreadsheet_id = data.get('spreadsheet_id')
    sheet_name = data.get('sheet_name')
    new_order = data.get('new_order')
    keep_selected_only = data.get('keep_selected_only', False)
    
    if not spreadsheet_id or not sheet_name or not new_order:
        return jsonify({'error': 'Spreadsheet ID, sheet name, or new order not provided.'}), 400
        
    try:
        service = build('sheets', 'v4', credentials=creds)
        
        # Get sheet ID
        sheet_metadata = with_retry(lambda: service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute())
        sheets = sheet_metadata.get('sheets', [])
        sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets if sheet['properties']['title'] == sheet_name), None)
        
        if sheet_id is None:
             return jsonify({'error': 'Sheet ID not found.'}), 404

        # 1. Get all data.
        range_name_data = f"'{sheet_name}'!A1:Z"
        result_data = with_retry(lambda: service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name_data).execute())
        all_data = result_data.get('values', [])
        
        if not all_data:
            return jsonify({'error': 'No data in the spreadsheet.'}), 404

        existing_header = all_data[0]
        
        # Reorder the data according to the final header order.
        reordered_data = []
        
        # Logic to handle the checkbox
        if keep_selected_only:
            SAVED_STATES[(spreadsheet_id, sheet_name)] = all_data
            
            existing_header_map = {col: i for i, col in enumerate(existing_header)}
            delete_indices = [i for i, col in enumerate(existing_header) if col not in new_order]
            
            delete_requests = []
            if delete_indices:
                delete_indices.sort(reverse=True)
                for index in delete_indices:
                    delete_requests.append({
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": index,
                                "endIndex": index + 1
                            }
                        }
                    })

            # Execute the batch update to delete the columns.
            if delete_requests:
                body = {'requests': delete_requests}
                with_retry(lambda: service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute())
            
            # Since the API call handles the deletion and shifting of columns, no need to clear or update values.
        
        else:
            seen_cols = set(new_order)
            final_header = new_order + [col for col in existing_header if col not in seen_cols]
            
            # Reorder all data rows based on the final header.
            for row in all_data:
                new_row = [row[existing_header.index(col_name)] if col_name in existing_header and existing_header.index(col_name) < len(row) else "" for col_name in final_header]
                reordered_data.append(new_row)

            body = {
                'values': reordered_data
            }
            with_retry(lambda: service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A1",
                valueInputOption='RAW',
                body=body
            ).execute())

        return jsonify({'message': 'Column order successfully saved.'})
    except HttpError as e:
        error_details = e.content.decode('utf-8')
        if 'Requested entity was not found' in error_details:
             return jsonify({'error': 'Invalid spreadsheet ID or not found.'}), 404
        elif 'The caller does not have permission' in error_details:
             return jsonify({'error': 'Please ensure the service account email has edit permission for the spreadsheet.'}), 403
        else:
             return jsonify({'error': f'An error occurred during the API call: {e}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# API endpoint: reverts the column changes.
@app.route('/api/revert', methods=['POST'])
def revert_columns():
    if not creds:
        return jsonify({'error': 'No authentication credentials.'}), 500

    data = request.json
    spreadsheet_id = data.get('spreadsheet_id')
    sheet_name = data.get('sheet_name')

    if not spreadsheet_id or not sheet_name:
        return jsonify({'error': 'Spreadsheet ID or sheet name not provided.'}), 400

    saved_data = SAVED_STATES.get((spreadsheet_id, sheet_name))
    if not saved_data:
        return jsonify({'error': 'No saved state to revert to. Please save the file again with the "Keep only selected columns" checkbox selected.'}), 404

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Clear existing data before reverting to avoid mixing old and new content.
        clear_range = f"'{sheet_name}'!A1:Z"
        with_retry(lambda: service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=clear_range
        ).execute())
        
        # Write the saved data back to the sheet.
        body = {
            'values': saved_data
        }
        with_retry(lambda: service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption='RAW',
            body=body
        ).execute())
        
        # Clear the saved state after a successful revert.
        del SAVED_STATES[(spreadsheet_id, sheet_name)]

        return jsonify({'message': 'Revert successful.'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Run the application.
if __name__ == '__main__':
    app.run(debug=True)
