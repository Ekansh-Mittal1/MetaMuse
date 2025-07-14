"""
BixBench benchmarking script for DendroForge.
Processes all test cases from the BixBench dataset and evaluates DendroForge responses.
"""

import ast
import asyncio
import json
import logging
import os
import shutil
import string
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
import yaml
from datasets import load_dataset
from huggingface_hub import hf_hub_download
from openai import AsyncOpenAI
from pydantic import BaseModel

from agents import (
    Agent,
    Runner,
    Model,
    ModelProvider,
    OpenAIChatCompletionsModel,
    RunConfig,
    ModelSettings,
    set_tracing_disabled,
)

# Load environment variables for scoring model
from dotenv import load_dotenv
load_dotenv(override=True)

SCORING_BASE_URL = os.getenv("OPENROUTER_BASE_URL")
SCORING_API_KEY = os.getenv("OPENROUTER_API_KEY")
SCORING_MODEL_NAME = "google/gemini-2.5-flash"

set_tracing_disabled(disabled=True)


class ScoringModelProvider(ModelProvider):
    """Custom model provider for scoring using Gemini."""

    def __init__(self):
        if not SCORING_BASE_URL or not SCORING_API_KEY:
            raise ValueError("Required environment variables for scoring model are not set.")
        self.client = AsyncOpenAI(base_url=SCORING_BASE_URL, api_key=SCORING_API_KEY)

    def get_model(self, model_name: str | None) -> Model:
        return OpenAIChatCompletionsModel(
            model=model_name or SCORING_MODEL_NAME,
            openai_client=self.client
        )


class QuestionScore(BaseModel):
    """Score for a single question."""
    question_id: str
    predicted_answer: str
    correct_answer: str
    is_correct: bool
    reasoning: str


class BixBenchScoreResult(BaseModel):
    """Complete scoring result for a BixBench test case."""
    capsule_id: str
    total_questions: int
    correct_answers: int
    accuracy: float
    question_scores: List[QuestionScore]
    overall_reasoning: str


class BixBenchScorer:
    """Agent for scoring BixBench test case responses."""

    def __init__(self):
        self.model_provider = ScoringModelProvider()
        self.logger = logging.getLogger(__name__)

    async def score_response(self, capsule_id: str, questions: List[Dict], dendroforge_output: str) -> BixBenchScoreResult:
        """Score a DendroForge response against ground truth answers."""
        prompt = self._create_scoring_prompt(capsule_id, questions, dendroforge_output)
        agent = Agent(
            name="BixBench Scorer",
            instructions="""You are an expert evaluator for the BixBench biological inference benchmark.
            Your task is to score DendroForge's responses against ground truth answers.

            For each question:
            1. Extract the predicted answer (A, B, C, or D, as well as the actual answer text) from the DendroForge output
            2. Compare it to the correct answer
            3. Provide a reasoning for the score

            Return your evaluation in the exact JSON format specified, without markdown formatting of ```json or ```. Just the JSON.""",
            model=self.model_provider.get_model(None)
        )

        result = await Runner.run(
            agent,
            input=prompt,
            run_config=RunConfig(
                model_provider=self.model_provider,
                model_settings=ModelSettings(
                    extra_body={"provider": {"order": ["google-vertex/us"]}}
                ),
            )
        )

        try:
            response_text = str(result.final_output)
            json_str = None
            brace_count = 0
            json_start = -1
            
            for i, char in enumerate(response_text):
                if char == '{':
                    if brace_count == 0:
                        json_start = i
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0 and json_start != -1:
                        json_str = response_text[json_start:i+1]
                        break
            
            if not json_str:
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                if json_start != -1 and json_end != -1:
                    json_str = response_text[json_start:json_end]

            if json_str:
                score_data = json.loads(json_str)
                question_scores = [QuestionScore(**qs) for qs in score_data.get('question_scores', [])]
                
                # Validate and correct totals
                actual_correct = sum(1 for q in question_scores if q.is_correct)
                if actual_correct != score_data.get('correct_answers'):
                    self.logger.warning(f"Correct answer count mismatch for {capsule_id}. Recalculating.")
                    score_data['correct_answers'] = actual_correct
                
                total_questions = len(question_scores)
                if total_questions != len(questions):
                     self.logger.warning(f"Question count mismatch for {capsule_id}. Expected {len(questions)}, found {total_questions}.")
                
                score_data['total_questions'] = total_questions
                score_data['accuracy'] = (actual_correct / total_questions) if total_questions > 0 else 0.0

                return BixBenchScoreResult(
                    capsule_id=score_data['capsule_id'],
                    total_questions=score_data['total_questions'],
                    correct_answers=score_data['correct_answers'],
                    accuracy=score_data['accuracy'],
                    question_scores=question_scores,
                    overall_reasoning=score_data.get('overall_reasoning', '')
                )
            else:
                raise ValueError("No valid JSON found in response")

        except Exception as e:
            self.logger.error(f"JSON parsing or validation failed for {capsule_id}: {e}")
            self.logger.debug(f"Response text: {response_text[:1000]}...")
            return BixBenchScoreResult(
                capsule_id=capsule_id,
                total_questions=len(questions),
                correct_answers=0,
                accuracy=0.0,
                question_scores=[],
                overall_reasoning=f"Scoring failed due to error: {str(e)}"
            )

    def _create_scoring_prompt(self, capsule_id: str, questions: List[Dict], dendroforge_output: str) -> str:
        """Create a prompt for the scoring agent."""
        prompt_parts = [
            "BIXBENCH SCORING TASK", "="*50, f"Capsule ID: {capsule_id}", "",
            "GROUND TRUTH ANSWERS:", ""
        ]
        for i, q in enumerate(questions, 1):
            prompt_parts.extend([f"Question {i} (ID: {q['id']}):", f"Question: {q['question']}", f"Correct Answer: {q['ideal_answer']}", ""])
        
        prompt_parts.extend([
            "DENDROFORGE OUTPUT (last 10k characters):", "="*50, dendroforge_output[-10000:], "",
            "TASK:",
            "1. For each question, extract DendroForge's predicted answer (A, B, C, or D, as well as the actual answer text) from the DendroForge output",
            "2. Compare it to the correct answer",
            "3. Provide a reasoning for the score",
            "4. IMPORTANT: First complete ALL individual question scores, then calculate the totals based on those individual results",
            "",
            "REQUIRED JSON OUTPUT FORMAT:",
            "STEP 1: Complete all individual question scores first",
            "STEP 2: Count the number of 'is_correct': true values to determine correct_answers",
            "STEP 3: Calculate accuracy as correct_answers / total_questions",
            "",
            json.dumps({
                "capsule_id": "capsule_id_here",
                "question_scores": [{
                    "question_id": "question_id_here",
                    "predicted_answer": "extracted_answer_text_or_UNCLEAR",
                    "correct_answer": "correct_answer_text",
                    "is_correct": True,
                    "reasoning": "explanation_of_scoring"
                }],
                "total_questions": "number_of_questions",
                "correct_answers": "count_of_true_is_correct_values_above",
                "accuracy": "correct_answers_divided_by_total_questions",
                "overall_reasoning": "overall_assessment_of_response_quality"
            }, indent=2)
        ])
        return "\n".join(prompt_parts)


class BixBenchDendroForge:
    """BixBench evaluation harness for DendroForge."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_dir = Path(config.get("data_dir", "bixbench_data"))
        self.log_dir = Path(config.get("log_dir", "tests/bixbench_logs"))
        
        self.data_dir.mkdir(exist_ok=True)
        self.capsules_dir = self.data_dir / "capsules"
        self.capsules_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_log_dir = self.log_dir / f"run_{self.run_timestamp}"
        self.run_log_dir.mkdir(exist_ok=True)
        
        # Save config to run log dir for reproducibility
        config_path = self.run_log_dir / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config, f, indent=2)

        self.scorer = BixBenchScorer()
        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration."""
        log_file = self.run_log_dir / "benchmark_run.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Starting BixBench run at {self.run_timestamp}")
        self.logger.info(f"Full logs will be available in: {self.run_log_dir}")

    def load_dataset(self) -> pd.DataFrame:
        """Load the BixBench dataset directly from HuggingFace."""
        self.logger.info("Loading dataset from HuggingFace...")
        try:
            ds = load_dataset("futurehouse/BixBench", split="train")
            df = ds.to_pandas()
            if isinstance(df['questions'].iloc[0], str):
                df['questions'] = df['questions'].apply(ast.literal_eval)
            self.logger.info(f"Loaded {len(df)} test cases from HuggingFace")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load dataset from HuggingFace: {e}")
            raise

    def download_capsule_data(self, capsule_row: pd.Series) -> Path:
        """Download and extract capsule data files."""
        zip_filename = capsule_row["data_folder"]
        extract_dir = self.capsules_dir / zip_filename.replace(".zip", "")
        if extract_dir.exists() and any(extract_dir.iterdir()):
            return extract_dir

        zip_path = self.capsules_dir / zip_filename
        if not zip_path.exists():
            self.logger.info(f"Downloading {zip_filename}...")
            hf_hub_download(
                repo_id="futurehouse/BixBench",
                filename=zip_filename,
                local_dir=str(self.capsules_dir),
                repo_type="dataset"
            )
        self.logger.info(f"Extracting {zip_filename}...")
        shutil.unpack_archive(zip_path, extract_dir)
        self._cleanup_capsule_structure(extract_dir)
        zip_path.unlink()
        return extract_dir

    def _cleanup_capsule_structure(self, extract_dir: Path):
        """Clean up the typical BixBench capsule structure."""
        for pattern in ["*Data*", "*Notebook*"]:
            for folder in extract_dir.rglob(pattern):
                if not folder.is_dir(): continue
                for item in folder.iterdir():
                    dest = extract_dir / item.name
                    if not dest.exists():
                        shutil.move(str(item), str(dest))
                shutil.rmtree(folder)
        for ipynb_file in extract_dir.rglob("*.ipynb"):
            ipynb_file.unlink()

    def create_dendroforge_prompt(self, capsule_row: pd.Series, data_dir: Path) -> str:
        """Create a prompt for DendroForge with all questions for a capsule."""
        prompt_parts = []
        
        abs_data_dir = data_dir.resolve()
        
        prompt_parts.extend([
            "="*80,
            f"BIXBENCH TEST CASE: {capsule_row['short_id']}",
            f"UUID: {capsule_row['uuid']}",
            "="*80, "",
            "RESEARCH HYPOTHESIS:", capsule_row['hypothesis'], "",
            "CATEGORIES:", str(capsule_row['categories']), "",
            "DATA LOCATION:", f"All data files for this analysis are located in: {abs_data_dir}", ""
        ])
        
        data_files = sorted([f for f in abs_data_dir.rglob("*") if f.is_file()])
        if data_files:
            prompt_parts.append("AVAILABLE DATA FILES:")
            for f in data_files[:20]:
                relative_path = f.relative_to(abs_data_dir)
                size_kb = f.stat().st_size / 1024
                prompt_parts.append(f"  - {relative_path} ({size_kb:.1f} KB)")
            if len(data_files) > 20:
                prompt_parts.append(f"  ... and {len(data_files) - 20} more files")
        else:
            prompt_parts.append("WARNING: No data files found in the specified directory")
        prompt_parts.append("")

        prompt_parts.extend([
            "QUESTIONS TO ANSWER:",
            "Please analyze the data and answer ALL of the following questions.",
            "For each question, provide your answer by selecting the best option (A, B, C, or D).", ""
        ])
        
        for i, q_dict in enumerate(capsule_row['questions'], 1):
            prompt_parts.extend([
                f"QUESTION {i} (ID: {q_dict['id']}):",
                q_dict['question'], "",
                "OPTIONS:"
            ])
            options = [q_dict['ideal_answer']] + [q_dict[f'distractor_{j}'] for j in range(1, 4)]
            import random
            random.seed(hash(q_dict['id']))
            random.shuffle(options)
            for j, option in enumerate(options):
                prompt_parts.append(f"  {string.ascii_uppercase[j]}. {option}")
            prompt_parts.extend(["", f"YOUR ANSWER FOR QUESTION {i}: [Select A, B, C, or D]", "", "-"*40, ""])

        prompt_parts.extend([
            "INSTRUCTIONS:",
            "1. Analyze all provided data files to answer the questions",
            "2. For each question, select the best answer from the options provided. You must select one and only one option for each question.",
            "3. In your final response, you must state the choice (in A,B,C,D) and the corresponding choice text, and provide clear reasoning for your answers.", ""
        ])
        return "\n".join(prompt_parts)

    async def call_dendroforge(self, prompt: str, capsule_id: str) -> Dict[str, Any]:
        """Call DendroForge via subprocess, redirecting output to a log file."""
        capsule_log_dir = self.run_log_dir / capsule_id
        capsule_log_dir.mkdir(exist_ok=True)
        
        dendroforge_output_path = capsule_log_dir / "dendroforge_output.log"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp_prompt:
            tmp_prompt.write(prompt)
            tmp_prompt_path = tmp_prompt.name

        response = {
            "capsule_id": capsule_id,
            "status": "error", "timestamp": datetime.now().isoformat()
        }
        process = None

        try:
            cmd = ['uv', 'run', 'main.py', 'run', 'qa', '--file', tmp_prompt_path]
            # Forward the requested model to DendroForge if specified in the config
            model_name: str | None = self.config.get("model_name")
            if model_name:
                cmd.extend(["--model", model_name])
            self.logger.info(f"Running DendroForge for {capsule_id}. Command: {' '.join(cmd)}")

            begin_time = datetime.now()
            
            with open(dendroforge_output_path, 'w') as f_out:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=f_out,
                    stderr=f_out
                )
            await asyncio.wait_for(process.wait(), timeout=18000)

            with open(dendroforge_output_path, 'r') as f_in:
                output = f_in.read()

            end_time = datetime.now()
            self.logger.info(f"DendroForge finished at {end_time}")
            self.logger.info(f"DendroForge took {end_time - begin_time} seconds")
            
            response.update({
                "status": "completed" if process.returncode == 0 else "failed",
                "returncode": process.returncode,
                "final_output": output,
                "begin_time": begin_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration": (end_time - begin_time).total_seconds()
            })

        except asyncio.TimeoutError:
            self.logger.error(f"DendroForge timed out for capsule {capsule_id}")
            if process:
                process.kill()
                await process.wait()
            response.update({"status": "timeout", "error": "Process timed out after 5 hours"})
        except Exception as e:
            self.logger.error(f"Failed to run DendroForge for capsule {capsule_id}: {e}")
            response["error"] = str(e)
        finally:
            if os.path.exists(tmp_prompt_path):
                os.unlink(tmp_prompt_path)

        # Save log after execution
        self.save_capsule_log(capsule_id, prompt, response)
        return response

    def save_capsule_log(self, capsule_id: str, prompt: str, response: Dict[str, Any]):
        """Save individual capsule log with prompt and response."""
        capsule_log_dir = self.run_log_dir / capsule_id
        log_data = {
            "capsule_id": capsule_id,
            "timestamp": datetime.now().isoformat(),
            "prompt": prompt,
            "response": {k: v for k, v in response.items() if k != "final_output"},
            "metadata": {"prompt_length": len(prompt), "response_status": response.get("status", "unknown")}
        }
        with open(capsule_log_dir / "full_log.json", 'w') as f:
            json.dump(log_data, f, indent=2)
        self.logger.info(f"Saved logs for {capsule_id} in {capsule_log_dir}")

    async def score_response(self, capsule_id: str, questions: List[Dict], dendroforge_output: str) -> BixBenchScoreResult:
        """Score a DendroForge response using the scoring agent."""
        if not dendroforge_output or not dendroforge_output.strip():
            self.logger.warning(f"Empty DendroForge output for capsule {capsule_id}")
            return BixBenchScoreResult(
                capsule_id=capsule_id, total_questions=len(questions), correct_answers=0,
                accuracy=0.0, question_scores=[], overall_reasoning="Empty DendroForge output"
            )
        
        # Check if output appears to be incomplete or error-filled
        output_lower = dendroforge_output.lower()
        error_indicators = [
            "error:", "failed", "not found", "cannot", "unable", 
            "timeout", "exception", "traceback", "stderr"
        ]
        
        if any(indicator in output_lower for indicator in error_indicators):
            self.logger.warning(f"DendroForge output for {capsule_id} appears to contain errors")
            
        # Check if output is very short (likely incomplete)
        if len(dendroforge_output.strip()) < 500:
            self.logger.warning(f"DendroForge output for {capsule_id} is very short ({len(dendroforge_output)} chars), may be incomplete")
        
        self.logger.info(f"Scoring response for capsule {capsule_id} (output length: {len(dendroforge_output)} chars)")
        score_result = await self.scorer.score_response(capsule_id, questions, dendroforge_output)
        self.save_score_results(capsule_id, score_result)
        return score_result

    def save_score_results(self, capsule_id: str, score_result: BixBenchScoreResult):
        """Save scoring results to the capsule's subfolder."""
        capsule_log_dir = self.run_log_dir / capsule_id
        with open(capsule_log_dir / "score_results.json", 'w') as f:
            json.dump(score_result.model_dump(), f, indent=2)
        
        summary_path = capsule_log_dir / "score_summary.txt"
        with open(summary_path, 'w') as f:
            f.write(f"BixBench Scoring Results for {capsule_id}\n"
                    f"Accuracy: {score_result.accuracy:.2%}\n"
                    f"Correct: {score_result.correct_answers}/{score_result.total_questions}\n\n"
                    f"Overall Assessment:\n{score_result.overall_reasoning}\n")
        self.logger.info(f"Saved scoring results for {capsule_id}")
    
    async def _process_capsule(self, row: pd.Series, semaphore: asyncio.Semaphore) -> Optional[Dict[str, Any]]:
        """Process a single BixBench test case capsule."""
        capsule_id = row['short_id']
        async with semaphore:
            self.logger.info(f"--- Starting processing for capsule: {capsule_id} ---")
            try:
                data_dir = self.download_capsule_data(row)
                prompt = self.create_dendroforge_prompt(row, data_dir)
                response = await self.call_dendroforge(prompt, capsule_id)

                score_result = None
                if response.get("status") == "completed":
                    dendroforge_output = response.get("final_output", "")
                    score_result = await self.score_response(capsule_id, row['questions'], dendroforge_output)

                capsule_result = {
                    "capsule_id": capsule_id, "status": response.get("status", "unknown"),
                    "accuracy": score_result.accuracy if score_result else None,
                    "correct_answers": score_result.correct_answers if score_result else None,
                    "scored": bool(score_result)
                }
                accuracy_str = f"{capsule_result['accuracy']:.2%}" if capsule_result['accuracy'] is not None else 'N/A'
                self.logger.info(f"--- Finished processing for capsule: {capsule_id} (Status: {capsule_result['status']}, Accuracy: {accuracy_str}) ---")
                return capsule_result

            except Exception as e:
                self.logger.error(f"Unhandled error processing capsule {capsule_id}: {e}", exc_info=True)
                return {"capsule_id": capsule_id, "status": "framework_error", "error": str(e), "scored": False}

    async def run_benchmark(self):
        """Run the complete benchmark based on the configuration."""
        self.logger.info("Starting BixBench benchmark run with config: %s", self.config)
        dataset = self.load_dataset()

        # Filter dataset based on config
        if self.config.get("single_test_id"):
            dataset = dataset[dataset['short_id'] == self.config["single_test_id"]].reset_index(drop=True)
            if dataset.empty: raise ValueError(f"Test case '{self.config['single_test_id']}' not found.")
        elif self.config.get("category_filter"):
            dataset = dataset[dataset['categories'].apply(lambda cats: any(c in self.config["category_filter"] for c in ast.literal_eval(cats)))].reset_index(drop=True)
        if self.config.get("limit"):
            dataset = dataset.head(self.config["limit"])
        
        self.logger.info(f"Processing {len(dataset)} test cases with concurrency={self.config.get('concurrency', 1)}")

        # Process capsules concurrently
        semaphore = asyncio.Semaphore(self.config.get("concurrency", 1))
        tasks = [self._process_capsule(row, semaphore) for _, row in dataset.iterrows()]
        capsule_results = [res for res in await asyncio.gather(*tasks) if res]

        # Aggregate and save final results
        self.summarize_results(capsule_results)

    def summarize_results(self, capsule_results: List[Dict[str, Any]]):
        """Calculates and saves the final summary of the benchmark run."""
        results = {
            "run_timestamp": self.run_timestamp, "config": self.config,
            "total_capsules": len(capsule_results),
            "completed": sum(1 for r in capsule_results if r["status"] == "completed"),
            "failed": sum(1 for r in capsule_results if r["status"] != "completed"),
            "scored": sum(1 for r in capsule_results if r.get("scored")),
            "capsule_results": capsule_results
        }
        
        scored_results = [r for r in capsule_results if r.get("scored")]
        if scored_results:
            accuracies = [r["accuracy"] for r in scored_results if r["accuracy"] is not None]
            if accuracies:
                results["scoring_summary"] = {
                    "mean_accuracy": sum(accuracies) / len(accuracies),
                    "min_accuracy": min(accuracies), "max_accuracy": max(accuracies),
                    "total_correct": sum(r.get("correct_answers", 0) for r in scored_results)
                }
        
        summary_file = self.run_log_dir / "benchmark_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        self.logger.info("=" * 60)
        self.logger.info("Benchmark Run Summary")
        self.logger.info("=" * 60)
        self.logger.info(f"Total capsules processed: {results['total_capsules']}")
        self.logger.info(f"  - Completed: {results['completed']}")
        self.logger.info(f"  - Failed: {results['failed']}")
        self.logger.info(f"  - Scored: {results['scored']}")
        if "scoring_summary" in results:
            summary = results["scoring_summary"]
            self.logger.info("\nScoring Summary:")
            self.logger.info(f"  - Mean Accuracy: {summary['mean_accuracy']:.2%}")
            self.logger.info(f"  - Total Correct: {summary['total_correct']}")
        self.logger.info(f"\nFull results saved in: {self.run_log_dir}")
        self.logger.info("=" * 60)


async def main():
    """Main entry point for the benchmark."""
    import argparse
    parser = argparse.ArgumentParser(description="Run BixBench evaluation with DendroForge")
    parser.add_argument("config_path", nargs='?', default="benchmarking/config_rnaseq_all.yaml", help="Path to the YAML configuration file.")
    args = parser.parse_args()

    try:
        with open(args.config_path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {args.config_path}")
        sys.exit(1)

    benchmark = BixBenchDendroForge(config)
    
    if config.get("score_existing"):
        print("Scoring existing run is not yet implemented in this version.")
        # To be implemented: logic for scoring a previous run.
        # await benchmark.score_existing_run(config["score_existing"])
    else:
        await benchmark.run_benchmark()


if __name__ == "__main__":
    asyncio.run(main())
