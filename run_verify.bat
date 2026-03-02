@echo off
echo STARTING VALIDATION at %TIME%
python src/core/mission_validation.py > validation_output_kernel.log 2>&1
echo ENDED VALIDATION at %TIME%
