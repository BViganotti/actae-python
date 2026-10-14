from pathlib import Path

from setuptools import find_packages, setup


ROOT = Path(__file__).parent
README = (ROOT / "README.md").read_text(encoding="utf-8")

setup(
    name="actae-client",
    version="1.3.0",
    author="Diviga Team",
    author_email="hello@diviga.dev",
    description="Actae Python SDK — record, replay, subscribe to agent events",
    license="MIT",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/BViganotti/actae-python",
    project_urls={
        "Documentation": "https://actae.dev/docs/sdks/python/",
        "Source": "https://github.com/BViganotti/actae-python",
        "Issues": "https://github.com/BViganotti/actae-python/issues",
    },
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.9",
    install_requires=[
        "aiohttp>=3.9",
    ],
    extras_require={
        "langgraph": ["langgraph>=1.0,<2"],
        "langchain": ["langchain-core>=0.3"],
        "crewai": ["crewai>=0.80"],
        "claude": ["claude-agent-sdk>=0.2"],
        "openai-agents": ["openai-agents>=0.19,<1"],
        "all": [
            "langgraph>=1.0,<2",
            "langchain-core>=0.3",
            "crewai>=0.80",
            "claude-agent-sdk>=0.2",
            "openai-agents>=0.19,<1",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
