# The following change needs to be upstreamed to TranscodingStreams.jl:

# We know the size of each output buffer (saved within Arrow metadata)
# The below functions mutates the provided output buffer.
function _transcode!(codec::Union{LZ4FrameDecompressor,LZ4FrameDecompressor}, data::TS.ByteData,output::TS.Buffer)
    input = TS.Buffer(data)
    error = TS.Error()
    code = TS.startproc(codec, :write, error)
    if code === :error
        @goto error
    end
    # n = TS.minoutsize(codec, buffermem(input))
    @label process
    # makemargin!(output, n)
    Δin, Δout, code = TS.process(codec, TS.buffermem(input), TS.marginmem(output), error)
    @debug(
        "called process()",
        code = code,
        input_size = buffersize(input),
        output_size = marginsize(output),
        input_delta = Δin,
        output_delta = Δout,
    )
    TS.consumed!(input, Δin)
    TS.supplied!(output, Δout)
    if code === :error
        @goto error
    elseif code === :end
        if TS.buffersize(input) > 0
            if TS.startproc(codec, :write, error) === :error
                @goto error
            end
            # n = minoutsize(codec, buffermem(input))
            @goto process
        end
        resize!(output.data, output.marginpos - 1)
        return output.data
    else
        # n = max(Δout, minoutsize(codec, buffermem(input)))
        @goto process
    end
    @label error
    if !(TS.haserror)(error)
        TS.set_default_error!(error)
    end
    throw(error[])
end