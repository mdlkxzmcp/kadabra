defmodule Kadabra.Connection.Settings do
  @moduledoc false

  defstruct enable_push: true,
            header_table_size: 4096,
            initial_window_size: 65_535,
            max_concurrent_streams: :infinite,
            max_frame_size: 16_384,
            max_header_list_size: nil

  alias Kadabra.Error

  @type t :: %__MODULE__{
    header_table_size: non_neg_integer,
    enable_push: boolean,
    max_concurrent_streams: non_neg_integer | :infinite,
    initial_window_size: non_neg_integer,
    max_frame_size: non_neg_integer,
    max_header_list_size: non_neg_integer
  }

  @table_header_size 0x1
  @enable_push 0x2
  @max_concurrent_streams 0x3
  @initial_window_size 0x4
  @max_frame_size 0x5
  @max_header_list_size 0x6

  @spec put(t, non_neg_integer, term) :: {:ok, t} | {:error, binary, t}
  def put(settings, @table_header_size, value) do
    {:ok, %{settings | header_table_size: value}}
  end

  def put(settings, @enable_push, 1) do
    {:ok, %{settings | enable_push: true}}
  end
  def put(settings, @enable_push, 0) do
    {:ok, %{settings | enable_push: false}}
  end
  def put(settings, @enable_push, _else) do
    {:error, Error.protocol_error, settings}
  end

  def put(settings, @max_concurrent_streams, value) do
    {:ok, %{settings | max_concurrent_streams: value}}
  end

  def put(settings, @initial_window_size, value) when value > 4_294_967_295 do
    {:error, Error.flow_control_error, settings}
  end
  def put(settings, @initial_window_size, value) do
    {:ok, %{settings | initial_window_size: value}}
  end

  def put(settings, @max_frame_size, value) do
    cond do
      value < 16_384 or value > 16_777_215 ->
        {:error, Error.protocol_error, settings}
      true ->
        {:ok, %{settings | max_frame_size: value}}
    end
  end

  def put(settings, @max_header_list_size, value) do
    {:ok, %{settings | max_header_list_size: value}}
  end

  def put(settings, _else, _value), do: {:ok, settings}

  def merge(old_settings, new_settings) do
    Map.merge(old_settings, new_settings, fn(k, v1, v2) ->
      cond do
        k == :__struct__ -> v1
        v1 == nil -> v2
        v2 == nil -> v1
        true -> v2
      end
    end)
  end
end
