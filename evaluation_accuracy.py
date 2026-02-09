import json
import subprocess
import os
from tqdm import tqdm

# --- CONFIGURATION ---
# 1. Path to your predictions file
PREDICTIONS_FILE = r"docspider_predictions.json"

# 2. MongoDB Connection String
MONGO_URI = ""

# 3. Path to Mongosh (FIXED with 'r' for raw string)
MONGO_SHELL_CMD = r"C:\mongosh-2.5.7-win32-x64\mongosh-2.5.7-win32-x64\bin\mongosh.exe"
# ---------------------

def clean_query(query):
    """Safety check: Prevents destructive commands."""
    forbidden = ["remove", "drop", "delete", "insert", "update", "save", "write"]
    if any(word in query.lower() for word in forbidden):
        return None 
    return query

def execute_mongo_query(db_name, query):
    """Executes MQL using the system's mongo shell."""
    query = clean_query(query)
    if not query:
        return "UNSAFE_OR_EMPTY"

    # Javascript wrapper to ensure we get clean JSON output
    js_script = f"""
    try {{
        var res = {query};
        if (res && typeof res.toArray === 'function') {{
            print(JSON.stringify(res.toArray()));
        }} else {{
            print(JSON.stringify(res));
        }}
    }} catch (e) {{
        print("ERROR: " + e.message);
    }}
    """

    uri_with_db = f"{MONGO_URI}/{db_name}"
    
    try:
        process = subprocess.run(
            [MONGO_SHELL_CMD, uri_with_db, "--quiet", "--eval", js_script],
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        
        output = process.stdout.strip()
        
        if "ERROR:" in output or process.returncode != 0:
            return f"EXECUTION_ERROR: {output}"
            
        return json.loads(output)

    except json.JSONDecodeError:
        return "JSON_PARSE_ERROR"
    except FileNotFoundError:
        return "MONGOSH_NOT_FOUND"
    except Exception as e:
        return f"SYSTEM_ERROR: {str(e)}"

def compare_results(gold_res, pred_res):
    """Compares two result sets, ignoring order for lists."""
    if isinstance(gold_res, str) or isinstance(pred_res, str): return False
    if gold_res is None or pred_res is None: return False

    # Simple types
    if isinstance(gold_res, (int, float)) and isinstance(pred_res, (int, float)):
        return gold_res == pred_res

    # Lists (Documents)
    if isinstance(gold_res, list) and isinstance(pred_res, list):
        if len(gold_res) != len(pred_res):
            return False
        
        if gold_res == pred_res: return True
            
        try:
            # Sort keys to ensure {a:1, b:2} == {b:2, a:1}
            gold_set = set(json.dumps(x, sort_keys=True) for x in gold_res)
            pred_set = set(json.dumps(x, sort_keys=True) for x in pred_res)
            return gold_set == pred_set
        except:
            return False

    return False

# --- MAIN EXECUTION ---
print("🚀 Starting Local Execution Accuracy Evaluation...")

if not os.path.exists(PREDICTIONS_FILE):
    print(f"❌ ERROR: File not found at {PREDICTIONS_FILE}")
    exit()

if not os.path.exists(MONGO_SHELL_CMD):
    print(f"❌ ERROR: mongosh.exe not found at {MONGO_SHELL_CMD}")
    exit()

print(f"📂 Loading predictions...")
with open(PREDICTIONS_FILE, 'r') as f:
    data = json.load(f)

correct_count = 0
total_count = 0
execution_errors = 0

with open("execution_mismatches.log", "w", encoding="utf-8") as log_file:
    for entry in tqdm(data):
        total_count += 1
        q_id = entry.get('question_id')
        db_id = entry.get('db_id')
        gold_mql = entry.get('gold_mql')
        pred_mql = entry.get('generated_mql')

        gold_result = execute_mongo_query(db_name=db_id, query=gold_mql)
        pred_result = execute_mongo_query(db_name=db_id, query=pred_mql)
        
        if isinstance(pred_result, str) and "ERROR" in pred_result:
            execution_errors += 1
            log_file.write(f"\n[ID: {q_id}] EXECUTION ERROR\nQuery: {pred_mql}\nError: {pred_result}\n")
            continue

        if compare_results(gold_result, pred_result):
            correct_count += 1
        else:
            log_file.write(f"\n[ID: {q_id}] MISMATCH\n")
            log_file.write(f"Gold: {gold_mql}\nPred: {pred_mql}\n")
            log_file.write(f"Gold Res: {str(gold_result)[:100]}...\n")
            log_file.write(f"Pred Res: {str(pred_result)[:100]}...\n")
            log_file.write("-" * 30 + "\n")

# --- REPORT ---
accuracy = (correct_count / total_count) * 100 if total_count > 0 else 0

print("\n" + "="*40)
print(f"📊 EXECUTION ACCURACY REPORT")
print("="*40)
print(f"Total:       {total_count}")
print(f"✅ Correct:   {correct_count}")
print(f"❌ Incorrect: {total_count - correct_count}")
print(f"⚠️ Errors:    {execution_errors}")
print("-" * 40)
print(f"🏆 ACCURACY:  {accuracy:.2f}%")

print("="*40)
