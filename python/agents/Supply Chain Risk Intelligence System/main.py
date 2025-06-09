#!/usr/bin/env python3
"""
ADK Compatibility Fix Script

This script fixes the `__name__` attribute error by ensuring all tool functions
have proper type annotations that ADK can parse correctly.
"""

import os
import sys
import re
from pathlib import Path

def fix_type_annotations(file_path: Path):
    """Fix type annotations in a Python file to be ADK compatible"""
    
    print(f"Fixing type annotations in: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Fix Optional type hints that cause issues with ADK
        # Replace Optional[Type] with Union[Type, None] or just Type = None
        content = re.sub(
            r'(\w+):\s*Optional\[([^\]]+)\]\s*=\s*None',
            r'\1: \2 = None',
            content
        )
        
        # Fix Optional imports
        content = re.sub(
            r'from typing import.*Optional.*',
            lambda m: m.group(0).replace('Optional, ', '').replace(', Optional', '').replace('Optional', ''),
            content
        )
        
        # Remove empty Optional imports
        content = re.sub(r'from typing import\s*$', '', content, flags=re.MULTILINE)
        
        # Fix function signatures that might cause issues
        # Replace complex Union types with simpler ones
        content = re.sub(
            r'Union\[([^,\]]+),\s*None\]',
            r'\1',
            content
        )
        
        # Ensure all async function parameters have proper defaults
        def fix_async_function_params(match):
            func_def = match.group(0)
            
            # Add default None for parameters that might be problematic
            problematic_params = [
                ('keywords: List[str]', 'keywords: List[str] = None'),
                ('ports: List[str]', 'ports: List[str] = None'),
                ('indicators: List[str]', 'indicators: List[str] = None'),
                ('vessel_types: List[str]', 'vessel_types: List[str] = None'),
                ('custom_keywords: List[str]', 'custom_keywords: List[str] = None'),
                ('geographic_focus: str', 'geographic_focus: str = None'),
                ('region: str', 'region: str = None'),
            ]
            
            for old_param, new_param in problematic_params:
                if old_param in func_def and new_param not in func_def:
                    func_def = func_def.replace(old_param, new_param)
            
            return func_def
        
        # Apply function parameter fixes
        content = re.sub(
            r'async def [^:]+:',
            fix_async_function_params,
            content,
            flags=re.MULTILINE | re.DOTALL
        )
        
        # Clean up imports that might be problematic
        lines = content.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Skip empty typing imports
            if 'from typing import' in line and len(line.split('import')[1].strip()) == 0:
                continue
            # Fix typing imports that are now empty
            if 'from typing import ' in line:
                imports = line.split('import')[1].strip()
                if not imports or imports in [',', ', ']:
                    continue
            cleaned_lines.append(line)
        
        content = '\n'.join(cleaned_lines)
        
        # Only write if content changed
        if content != original_content:
            # Create backup
            backup_path = file_path.with_suffix(f'{file_path.suffix}.backup')
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(original_content)
            print(f"  Created backup: {backup_path}")
            
            # Write fixed content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  ✅ Fixed type annotations")
        else:
            print(f"  ℹ️  No changes needed")
            
    except Exception as e:
        print(f"  ❌ Error fixing {file_path}: {e}")

def main():
    """Main fix function"""
    print("="*60)
    print("ADK Compatibility Fix - Type Annotation Fixer")
    print("="*60)
    
    # Find Python files to fix
    current_dir = Path.cwd()
    
    files_to_fix = [
        current_dir / "root_agent" / "tools.py",
        current_dir / "sub_agents" / "data_collector" / "tools.py",
        current_dir / "sub_agents" / "data_collector" / "agent.py",
        current_dir / "root_agent" / "agent.py",
    ]
    
    # Also find any other Python files with tool functions
    for py_file in current_dir.rglob("*.py"):
        if "tools.py" in py_file.name or "agent.py" in py_file.name:
            if py_file not in files_to_fix:
                files_to_fix.append(py_file)
    
    print(f"Found {len(files_to_fix)} files to check:")
    for file_path in files_to_fix:
        if file_path.exists():
            print(f"  - {file_path}")
        else:
            print(f"  - {file_path} (not found)")
    print()
    
    # Fix each file
    fixed_count = 0
    for file_path in files_to_fix:
        if file_path.exists():
            fix_type_annotations(file_path)
            fixed_count += 1
    
    print()
    print("="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Files processed: {fixed_count}")
    print("✅ Type annotation fixes applied")
    print("📄 Backup files created for modified files")
    print()
    print("Next steps:")
    print("1. Run: poetry run adk web")
    print("2. Select root_agent")
    print("3. Test the system")
    print()
    print("If issues persist, check the error logs and compare with backup files")

if __name__ == "__main__":
    main()