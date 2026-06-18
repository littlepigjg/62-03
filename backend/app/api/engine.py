import asyncio
from typing import List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from datetime import datetime

from ..config import settings
from ..models import (
    EngineExecuteRequest,
    BatchResumeRequest,
    JobProgressResponse,
    TaskResultResponse,
    JobPlanResponse,
    ServerHeatmapResponse,
)
from ..core.parallel_engine import engine, TaskStatus
from ..core.stream import stream_manager, StreamMessage

router = APIRouter(prefix="/engine", tags=["Parallel Engine"])


def _register_engine_callbacks(job_id: str) -> None:
    async def _output_cb(
        jid: str,
        server_id: str,
        server_name: str,
        stream: str,
        content: str,
    ) -> None:
        try:
            loop = asyncio.get_event_loop()
            if not loop.is_running():
                return

            msg = StreamMessage(
                type="output",
                task_id=f"{jid}-{server_id}",
                server_id=server_id,
                server_name=server_name,
                stream=stream,
                content=content,
            )
            await stream_manager._dispatch(msg)
        except Exception:
            pass

    def _sync_output_cb(
        jid: str,
        server_id: str,
        server_name: str,
        stream: str,
        content: str,
    ) -> None:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    _output_cb(jid, server_id, server_name, stream, content),
                    loop,
                )
        except RuntimeError:
            pass

    def _status_cb(jid: str, progress: dict) -> None:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    _async_status_cb(jid, progress),
                    loop,
                )
        except RuntimeError:
            pass

    async def _async_status_cb(jid: str, progress: dict) -> None:
        msg = StreamMessage(
            type="engine_status",
            task_id=jid,
            server_id="",
            server_name="",
            status=progress["status"],
            content="",
        )
        await stream_manager._dispatch(msg)

    engine.register_output_callback(_sync_output_cb)
    engine.register_status_callback(_status_cb)


@router.post("/command", response_model=JobPlanResponse)
async def engine_execute_command(req: EngineExecuteRequest, background_tasks: BackgroundTasks):
    if not req.command:
        raise HTTPException(status_code=400, detail="Command is required")

    servers = []
    for sid in req.server_ids:
        server = settings.get_server(sid)
        if not server:
            raise HTTPException(status_code=404, detail=f"Server '{sid}' not found")
        servers.append(server)

    if not servers:
        raise HTTPException(status_code=400, detail="No valid servers specified")

    engine.max_batch_size = req.max_batch_size
    plan = engine.execute_commands(
        servers=servers,
        command=req.command,
        name=req.name,
        timeout=req.timeout,
        env=req.env,
        grouping_strategy=req.grouping_strategy,
        max_retries=req.max_retries,
        order_by=req.order_by,
    )

    _register_engine_callbacks(plan.job_id)

    return plan.to_dict()


@router.post("/script", response_model=JobPlanResponse)
async def engine_execute_script(req: EngineExecuteRequest, background_tasks: BackgroundTasks):
    if not req.script_content:
        raise HTTPException(status_code=400, detail="Script content is required")

    servers = []
    for sid in req.server_ids:
        server = settings.get_server(sid)
        if not server:
            raise HTTPException(status_code=404, detail=f"Server '{sid}' not found")
        servers.append(server)

    if not servers:
        raise HTTPException(status_code=400, detail="No valid servers specified")

    engine.max_batch_size = req.max_batch_size
    plan = engine.execute_scripts(
        servers=servers,
        script_content=req.script_content,
        script_name=req.script_name or "script.sh",
        interpreter=req.interpreter,
        args=req.args,
        name=req.name,
        timeout=req.timeout,
        grouping_strategy=req.grouping_strategy,
        max_retries=req.max_retries,
        order_by=req.order_by,
    )

    _register_engine_callbacks(plan.job_id)

    return plan.to_dict()


@router.get("/jobs", response_model=List[JobPlanResponse])
async def list_jobs():
    jobs = engine.get_all_jobs()
    return [job.to_dict() for job in jobs]


@router.get("/jobs/{job_id}/progress", response_model=JobProgressResponse)
async def get_job_progress(job_id: str):
    try:
        progress = engine.get_job_progress(job_id)
        return progress.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/jobs/{job_id}/results", response_model=List[TaskResultResponse])
async def get_job_results(job_id: str, order_by: str = "server_id"):
    try:
        ordered_results = engine.get_ordered_output(job_id, order_by=order_by)
        results = []
        for _, result in ordered_results:
            results.append(TaskResultResponse(
                task_id=result.task_id,
                server_id=result.server_id,
                server_name=result.server_name,
                exit_code=result.exit_code,
                stdout=result.stdout,
                stderr=result.stderr,
                start_time=result.start_time.isoformat() if result.start_time else None,
                end_time=result.end_time.isoformat() if result.end_time else None,
                status=result.status.value,
                retry_count=result.retry_count,
                duration=result.duration,
                error_message=result.error_message,
            ))
        return results
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/batches/resume")
async def resume_batch(req: BatchResumeRequest):
    success = engine.resume_batch(req.batch_id)
    if not success:
        raise HTTPException(status_code=400, detail="Batch not found or not paused")
    return {"status": "success", "message": f"Batch {req.batch_id} resumed"}


@router.get("/heatmap", response_model=List[ServerHeatmapResponse])
async def get_server_heatmap():
    heatmap_data = engine.get_server_heatmap()
    return list(heatmap_data.values())


@router.post("/shutdown/graceful")
async def shutdown_graceful():
    engine.shutdown_graceful(wait=False)
    return {"status": "success", "message": "Graceful shutdown initiated"}


@router.post("/shutdown/force")
async def shutdown_force():
    engine.shutdown_force()
    return {"status": "success", "message": "Force shutdown initiated"}


@router.get("/status")
async def get_engine_status():
    return {
        "max_workers": engine.max_workers,
        "max_batch_size": engine.max_batch_size,
        "failure_threshold": engine.failure_threshold,
        "current_concurrency": engine._controller.current_concurrency,
        "active_tasks": len(engine._active_tasks),
        "queued_tasks": len(engine._task_queue),
        "is_shutdown": engine._shutdown.is_set(),
        "is_graceful_shutdown": engine._graceful_shutdown.is_set(),
        "controller_stats": engine._controller.get_stats(),
    }
