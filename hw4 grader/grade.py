#!/usr/bin/env python3.6

import subprocess
import enum
import collections
import time
import sys


class TestResultType(enum.Enum):
    TRANSFER_ERROR = 0
    TRANSFER_TIMEOUT = 1
    SLOW_TRANSFER = 2
    FAST_TRANSFER = 3

TestArguments = collections.namedtuple('TestArguments', "file loss delay")
TestCase = collections.namedtuple("TestCase", "args secs")
TestResult = collections.namedtuple("TestResult", "type duration")

def run_test_case(case: TestCase) -> TestResult:
    full_credit_time = case.secs * 1.5
    half_credit_time = case.secs * 2.25
    start_time = time.time()

    test_args = ["python3.6", "tester.py", "--file", case.args.file, "--loss",
                 case.args.loss, "--delay", case.args.delay]

    try:
        subprocess.run(test_args, timeout=half_credit_time, check=True,
                       stdout=subprocess.PIPE)
    except subprocess.TimeoutExpired:
        return TestResult(TestResultType.TRANSFER_TIMEOUT, None)
    except subprocess.CalledProcessError:
        return TestResult(TestResultType.TRANSFER_ERROR, None)

    end_time = time.time()
    duration = end_time - start_time
    if duration <= full_credit_time:
        result_type = TestResultType.FAST_TRANSFER
    else:
        result_type = TestResultType.SLOW_TRANSFER
    return TestResult(result_type, duration)


TEST_CASES = [
    TestCase(TestArguments("grading_data/test-1.gif", "0", "1"), 62.7),
    TestCase(TestArguments("grading_data/test-2.gif", "0", "0.1"), 6),
    TestCase(TestArguments("grading_data/test-3.jpg", "0", ".01"), 4),
    TestCase(TestArguments("grading_data/test-4.png", "0.05", ".1"), 8.7),
    TestCase(TestArguments("grading_data/test-5.gif", "0.05", ".01"), 7.3),
    TestCase(TestArguments("grading_data/test-6.png", "0.1", ".01"), 17.3),
]

TOTAL_SCORE = 0
MAX_SCORE = 12
for CASE_NUM, A_CASE in enumerate(TEST_CASES):
    FULL_CREDIT_TIME = A_CASE.secs * 1.5
    HALF_CREDIT_TIME = FULL_CREDIT_TIME * 1.5
    print("Test case: {}".format(CASE_NUM + 1))
    print("  - File: {}".format(A_CASE.args.file))
    print("  - Loss: {}%".format(A_CASE.args.loss))
    print("  - Delay: {} seconds".format(A_CASE.args.delay))
    print("  - Full credit: {} secs".format(round(FULL_CREDIT_TIME)))
    print("  - Half credit: {} secs".format(round(HALF_CREDIT_TIME)))
    print("----------")

    CASE_RESULT = run_test_case(A_CASE)
    if CASE_RESULT.type == TestResultType.TRANSFER_ERROR:
        print("  ! 0 points, file was transferred incorrectly")
    elif CASE_RESULT.type == TestResultType.TRANSFER_TIMEOUT:
        print("  ! 0 points, file not transferred in {} secs".format(
            round(HALF_CREDIT_TIME)))
    elif CASE_RESULT.type == TestResultType.SLOW_TRANSFER:
        print("  ~ 1 point, transferred in {} secs".format(round(CASE_RESULT.duration)))
        TOTAL_SCORE += 1
    elif CASE_RESULT.type == TestResultType.FAST_TRANSFER:
        print("  * 2 points, transferred in {} secs".format(round(CASE_RESULT.duration)))
        TOTAL_SCORE += 2
    print("")

print("==========")
print("Total Score: {} / {}".format(TOTAL_SCORE, MAX_SCORE))
sys.exit(0 if TOTAL_SCORE == MAX_SCORE else 1)
