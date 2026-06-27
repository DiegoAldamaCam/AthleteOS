interface ErrorAlertProps {
  message: string
}

export default function ErrorAlert({ message }: ErrorAlertProps) {
  return (
    <div role="alert">
      {message}
    </div>
  )
}
