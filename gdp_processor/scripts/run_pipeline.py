"""
Pipeline Orchestrator
Run all stages sequentially or individually
"""

import sys
from pathlib import Path
import logging
import argparse

# Import config
sys.path.append(str(Path(__file__).parent.parent))
from config import LOG_FORMAT, LOG_LEVEL

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def run_stage(stage_num: int):
    """Run a specific stage"""
    stage_scripts = {
        1: "stage_01_normalize.py",
        2: "stage_02_rebase.py",
        3: "stage_03_panel.py",
        4: "stage_04_impute.py",
        5: "stage_05_summarize.py",
    }
    
    if stage_num not in stage_scripts:
        logger.error(f"Invalid stage number: {stage_num}")
        return False
    
    script_name = stage_scripts[stage_num]
    logger.info(f"\n{'='*60}")
    logger.info(f"Running Stage {stage_num}: {script_name}")
    logger.info(f"{'='*60}\n")
    
    # Import and run the stage
    try:
        script_path = Path(__file__).parent / script_name
        if not script_path.exists():
            logger.warning(f"Stage script not yet implemented: {script_name}")
            return False
        
        # Execute the script
        exec(open(script_path).read(), {'__name__': '__main__'})
        return True
    
    except Exception as e:
        logger.error(f"Error running stage {stage_num}: {e}")
        return False


def main():
    """Main pipeline orchestrator"""
    parser = argparse.ArgumentParser(description="GDP Data Processing Pipeline")
    parser.add_argument(
        '--stage', 
        type=int, 
        choices=[1, 2, 3, 4, 5],
        help='Run a specific stage (1-5). If not specified, runs all stages.'
    )
    parser.add_argument(
        '--start-from',
        type=int,
        choices=[1, 2, 3, 4, 5],
        help='Start from a specific stage and run to completion'
    )
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("GDP Data Processing Pipeline")
    logger.info("="*60)
    
    if args.stage:
        # Run single stage
        logger.info(f"Mode: Single stage ({args.stage})")
        success = run_stage(args.stage)
        if not success:
            sys.exit(1)
    
    elif args.start_from:
        # Run from specific stage to end
        logger.info(f"Mode: Start from stage {args.start_from}")
        for stage in range(args.start_from, 6):
            success = run_stage(stage)
            if not success:
                logger.error(f"Pipeline stopped at stage {stage}")
                sys.exit(1)
    
    else:
        # Run all stages
        logger.info("Mode: Full pipeline (stages 1-5)")
        for stage in range(1, 6):
            success = run_stage(stage)
            if not success:
                logger.error(f"Pipeline stopped at stage {stage}")
                sys.exit(1)
    
    logger.info("\n" + "="*60)
    logger.info("Pipeline Complete!")
    logger.info("="*60)


if __name__ == "__main__":
    main()
