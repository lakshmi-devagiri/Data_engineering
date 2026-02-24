import os
import re
from pathlib import Path
from typing import List, Dict

# LangChain (new-style imports)
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

import pandas as pd  # pip install pandas

# ---- Safety: require API key via env var ----
if not os.getenv("OPENAI_API_KEY"):
    raise RuntimeError(
        "Set your OpenAI key in the environment first, e.g.\n"
        '  setx OPENAI_API_KEY "sk-..."   (Windows PowerShell)\n'
        "Then restart your terminal/IDE."
    )

def load_questions(path: str | Path) -> List[str]:
    """Load questions from a text file. Supports:
       - one-per-line
       - paragraphs with sentences ending in '?'
       Removes numbering like '1) ' / '2. ' etc.
    """
    text = Path(path).read_text(encoding="utf-8")
    with_q = re.findall(r"([^\n?.!]*\?)(?=\s|$)", text)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    candidates = with_q if with_q else lines

    out, seen = [], set()
    for q in candidates:
        q = re.sub(r"^\s*\d+[\).\-\:]\s*", "", q).strip()
        if q and q not in seen:
            seen.add(q)
            out.append(q)
    return out

def build_chain(model_name: str = "gpt-4o-mini"):
    system = (
        "You are a concise, accurate assistant. "
        "Answer each question directly; if info is missing, say what's needed."
    )
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system),
            ("user", "Question: {question}\n\nAnswer briefly and clearly."),
        ]
    )
    llm = ChatOpenAI(model=model_name, temperature=0)
    return prompt | llm | StrOutputParser()

def answer_questions_file(
    input_path: str | Path,
    output_csv: str | Path = "answers.csv",
    model_name: str = "gpt-4o-mini",
    max_concurrency: int = 5,
) -> pd.DataFrame:
    qs = load_questions(input_path)
    if not qs:
        raise ValueError("No questions found in the input file.")

    chain = build_chain(model_name)
    inputs = [{"question": q} for q in qs]
    answers = chain.batch(inputs, {"max_concurrency": max_concurrency})

    rows: List[Dict[str, str]] = [{"question": q, "answer": a.strip()} for q, a in zip(qs, answers)]
    df = pd.DataFrame(rows, columns=["question", "answer"])
    df.to_csv(output_csv, index=False, encoding="utf-8")
    return df

if __name__ == "__main__":
    # Put your questions in this file:
    input_file = r"D:\bigdata\drivers\assignments.txt"
    df = answer_questions_file(input_file, "answers.csv", "gpt-4o-mini", 5)
    print(f"Saved {len(df)} answers to answers.csv")
