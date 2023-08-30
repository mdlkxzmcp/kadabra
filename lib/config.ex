defmodule Kadabra.Config do
  @moduledoc false

  @type t :: %__MODULE__{
          client: pid,
          encoder: pid,
          decoder: pid,
          queue: pid,
          uri: URI.t(),
          socket: pid,
          opts: Keyword.t()
        }

  defstruct client: nil,
            encoder: nil,
            decoder: nil,
            queue: nil,
            uri: nil,
            socket: nil,
            opts: []
end
